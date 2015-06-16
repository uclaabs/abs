/*
 * Copyright (C) 2012 The Regents of The University California. 
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.execution

import java.util.{HashMap => JHashMap}

import com.googlecode.javaewah.EWAHCompressedBitmap
import it.unimi.dsi.fastutil.ints.IntArrayList
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.abm.AbmUtilities
import org.apache.hadoop.hive.ql.abm.datatypes._
import org.apache.hadoop.hive.ql.abm.simulation.{CovOracle, MCSimNode, PartialCovMap, TupleMap}
import org.apache.hadoop.hive.ql.abm.udf.GenRowId
import org.apache.hadoop.hive.ql.abm.udf.simulation.{SimulationSamples, GenericUDFWithSimulation}
import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, ExprNodeEvaluatorFactory}
import org.apache.hadoop.hive.ql.plan.{ExprNodeGenericFuncDesc, SelectDesc}
import org.apache.hadoop.hive.serde2.objectinspector.{StructObjectInspector, ObjectInspector}
import org.apache.hadoop.io.Writable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{RDD, ZippedLookupRDD}
import org.apache.spark.storage.StorageLevel
import shark.SharkEnv
import shark.api.QueryExecutionException
import shark.execution.abm.FinalTupleParser
import shark.execution.serialization.{OperatorSerializationWrapper, SerializableWritable}
import shark.memstore2._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.BeanProperty


/**
 * An operator that does projection, i.e. selecting certain columns and
 * filtering out others.
 */
class SelectOperator extends UnaryOperator[SelectDesc] {

  @BeanProperty var conf: SelectDesc = _

  @transient var evals: Array[ExprNodeEvaluator] = _

  // ABM
  @BeanProperty var localHconf: HiveConf = _
  @transient var internalOutputOI: ObjectInspector = _

  @transient var genId: Option[_] = _
  @transient var samples: SimulationSamples = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    conf = desc
    // ABM
    localHconf = super.hconf
    initializeEvals(false)
  }

  def initializeEvals(initializeEval: Boolean) {
    if (!conf.isSelStarNoCompute) {
      evals = conf.getColList().map(ExprNodeEvaluatorFactory.get(_)).toArray
      if (initializeEval) {
        evals.foreach(_.initialize(objectInspector))
        // ABM
        if (conf.isSimulated) {
          CovOracle.NUM_TUTPLES = conf.getTotalTupleNumber
          MCSimNode.setNumSimulations(conf.getNumSimulation)

          samples = new SimulationSamples
          conf.getColList.zip(conf.getAggrColIdxs).foreach { case (expr, idx) =>
            if (idx != -1) {
              val udf = expr.asInstanceOf[ExprNodeGenericFuncDesc].getGenericUDF.asInstanceOf[GenericUDFWithSimulation]
              udf.setNumSimulation(conf.getNumSimulation)
              udf.setSamples(samples)
              udf.setColumnIndex(idx)
            }
          }
        }
      }
    }
    // ABM: find the GenRowId function
    genId = conf.getColList.find(expr =>
      expr.isInstanceOf[ExprNodeGenericFuncDesc]
        && expr.asInstanceOf[ExprNodeGenericFuncDesc].getGenericUDF.isInstanceOf[GenRowId])
  }

  override def initializeOnSlave() {
    initializeEvals(true)
    // ABM
    internalOutputOI = internalOutputObjectInspector()
  }

  override def processPartition(split: Int, iter: Iterator[_]) = {
    if (conf.isSelStarNoCompute) {
      iter
    } else {
      // ABM: set the split Id
      genId.foreach {
        _.asInstanceOf[ExprNodeGenericFuncDesc].getGenericUDF.asInstanceOf[GenRowId].setSplitId(split)
      }

      val reusedRow = new Array[Object](evals.length)
      iter.map { row =>
        var i = 0
        while (i < evals.length) {
          reusedRow(i) = evals(i).evaluate(row)
          i += 1
        }
        reusedRow
      }
    }
  }

  override def outputObjectInspector(): ObjectInspector = {
    if (conf.isCached) {
      val tableSerDe = new ColumnarSerDe
      tableSerDe.initialize(localHconf, getConf.getTableDesc.getProperties)
      tableSerDe.getObjectInspector()
    } else {
      internalOutputObjectInspector()
    }
  }

  private def internalOutputObjectInspector(): ObjectInspector = {
    if (conf.isSelStarNoCompute()) {
      super.outputObjectInspector()
    } else {
      initEvaluatorsAndReturnStruct(evals, conf.getOutputColumnNames(), objectInspector)
    }
  }

  override def execute(): RDD[_] = {
    if (conf.isCached) {
      val databaseName = AbmUtilities.ABM_CACHE_DB_NAME
      val tableName = conf.getTableName
      if (!SharkEnv.memoryMetadataManager.containsTable(databaseName, tableName)) {
        // Populate the output table and cache it
        populateAndCache(databaseName, tableName)
      }
      // Read the cached table and return it
      readFromCache(databaseName, tableName)
    }
    else if (conf.isSimulated) {
      // val startCollect = System.currentTimeMillis()
      val (localSrvs, localPCovMap) = if (conf.isSimpleQuery) broadcastCache else broadcastCachedAndComputeCovariance
      // val collectTime = System.currentTimeMillis() - startCollect
      // println("Broadcast took " + collectTime + " ms")

      val srvs: Broadcast[Array[TupleMap]] = SharkEnv.sc.broadcast(localSrvs)
      val pCovMap: Broadcast[PartialCovMap] = SharkEnv.sc.broadcast(localPCovMap)

      val inputRdd = parentOperator.execute()
      val rddPreprocessed = preprocessRdd(inputRdd)
      val rddProcessed = executeSimulation(rddPreprocessed, srvs, pCovMap)
      postprocessRdd(rddProcessed)
    } else {
      super.execute()
    }
  }

  private def populateAndCache(databaseName: String, tableName: String) {
    val inputRdd = if (parentOperators.size == 1) executeParents().head._2 else null
    val op = OperatorSerializationWrapper(this)

    val outputRdd = inputRdd.mapPartitionsWithIndex { case (split, partition) =>
      op.logDebug("Started executing mapPartitions for operator: " + op)
      op.logDebug("Input object inspectors: " + op.objectInspectors)

      op.initializeOnSlave()
      val newPart = op.processPartition(split, partition)
      op.logDebug("Finished executing mapPartitions for operator: " + op)

      val serde = new ColumnarSerDe
      serde.initialize(op.localHconf, op.getConf.getTableDesc.getProperties)

      // Serialize each row into the builder object.
      // ColumnarSerDe will return a TablePartitionBuilder.
      var builder: Writable = null
      newPart.foreach { row =>
        builder = serde.serialize(row.asInstanceOf[AnyRef], op.internalOutputOI)
      }

      if (builder == null) {
        // Empty partition.
        Iterator(new TablePartition(0, Array()))
      } else {
        Iterator(builder.asInstanceOf[TablePartitionBuilder].build)
      }
    }

    //    val startCollect = System.currentTimeMillis()

    // Run a job on the RDD that contains the query output to force the data into the memory
    // store. The statistics will also be collected by 'statsAcc' during job execution.
    outputRdd.persist(StorageLevel.MEMORY_AND_DISK)
    outputRdd.context.runJob(
      outputRdd, (iter: Iterator[TablePartition]) => iter.foreach(_ => Unit))

    outputRdd.setName(tableName)
    // Create a new MemoryTable entry if one doesn't exist (i.e., this operator is for a CTAS).
    val memoryTable = SharkEnv.memoryMetadataManager.createMemoryTable(
      databaseName, tableName, CacheType.MEMORY)
    memoryTable.put(outputRdd, Map.empty)

    // val collectTime = System.currentTimeMillis() - startCollect
    // println("Caching took " + collectTime + " ms")
  }

  private def readFromCache(databaseName: String, tableName: String): RDD[ColumnarStruct] = {
    val tableOpt = SharkEnv.memoryMetadataManager.getMemoryTable(databaseName, tableName)
    if (tableOpt.isEmpty) {
      logError( """|Table %s.%s not found in block manager.
                  |Are you trying to access a cached table from a Shark session other than the one
                  |in which it was created?""".stripMargin.format(databaseName, tableName))
      throw new QueryExecutionException("Cached table not found")
    }

    val numCols = conf.getColList.size()
    val table = tableOpt.get
    val tableRdd = table.getRDD.get
    tableRdd.mapPartitions { iter =>
      if (iter.hasNext) {
        val columnsUsed = TablePartitionIterator.newBitSet(numCols)
        val tablePartition = iter.next()
        tablePartition.prunedIterator(columnsUsed)
      } else {
        Iterator.empty
      }
    }
  }

  private def broadcastCachedAndComputeCovariance(): (Array[TupleMap], PartialCovMap) = {
    val op1 = OperatorSerializationWrapper(this)

    val numKeysC = conf.getNumKeysContinuous
    val lenC = numKeysC.size()

    val (key2LinId, id2AggrsCondC) = numKeysC.zip(Range(1 + lenC, 1 + lenC * 2)).map { case (numKeys, idx) =>
      val op = op1
      val table = parentOperators(idx).execute().mapPartitions { iter =>
        val parser = new ContinuousOutputParser(op.objectInspectors.get(idx), numKeys)
        iter.map(t => parser.parse(t))
      }.collect()

      // println("---------------BEGIN: continuous output " + idx + "-----------------------------")
      val hash1 = new JHashMap[ArrayBuffer[SerializableWritable[Writable]], (Array[EWAHCompressedBitmap], Int)]()
      val hash2 = new TupleMap
      table.foreach { t =>
        // println("(" + t._1.toString  + ", " + t._2.mkString("[", ",", "]")  + ", "
        //     + t._3.map(_.toArray.mkString("[", ",", "]")).mkString("{", ";", "}")  + ", ("
        //     + t._4.toString  + ", " + t._5.toString + "), " + t._6.toString + ")")
        // key: (lineage, id)
        hash1.put(t._1, (t._3, t._6))
        // id: (aggrs, cond)
        hash2.put(t._6, new SrvTuple(t._2, t._4, t._5))
      }
      // println("---------------END: continuous output " + idx + "-----------------------------")

      (hash1, hash2)
    }.unzip

    val numKeysD = conf.getNumKeysDiscrete
    val lenD = numKeysD.size()

    val id2AggrsCondD = numKeysD.zip(Range(1 + lenC * 2, 1 + lenC * 2 + lenD)).map { case (numKeys, idx) =>
      val op = op1
      val table = parentOperators(idx).execute().mapPartitions { iter =>
        val parser = new DiscreteOutputParser(op.objectInspectors.get(idx), numKeys)
        iter.map(t => parser.parse(t))
      }.collect()

      // println("---------------BEGIN: discrete output " + idx + "-----------------------------")
      val hash = new TupleMap
      table.foreach { t =>
        // println("(" + t._1.mkString("[", ",", "]")  + ", (" + t._2.toString  + ", " + t._3.toString  + "), " + t._4.toString + ")")
        // id: (aggrs, cond)
        hash.put(t._4, new SrvTuple(t._1, t._2, t._3))
      }
      // println("---------------END: discrete output " + idx + "-----------------------------")

      hash
    }

    // val databaseName = AbmUtilities.ABM_CACHE_DB_NAME
    // conf.getCachedOutputs.foreach(SharkEnv.memoryMetadataManager.removeTable(databaseName, _))

    val lookup = SharkEnv.sc.broadcast(key2LinId)
    val rdds = parentOperators.slice(1, 1 + lenC).toArray.map(_.execute())
    val pCovMap: PartialCovMap = new ZippedLookupRDD(op1, lookup, SharkEnv.sc, rdds).mapPartitions(iter => {
      val sizes: Array[Int] = op1.conf.getAggrTypesContinuous.map(_.size()).toArray
      val ret = new PartialCovMap(sizes)
      iter.foreach(t => ret.iterate(t.tid, t.gbys, t.groupIds, t.lineages, t.vals))
      Array(ret).iterator
    }).collect().reduce { (x, y) =>
      x.merge(y)
      x
    }
    // println("---------------BEGIN: partial covariance map -----------------------------")
    // println(pCovMap)
    // println("---------------END: partial covariance map -----------------------------")

    // conf.getCachedInputs.foreach(SharkEnv.memoryMetadataManager.removeTable(databaseName, _))

    ((id2AggrsCondC ++ id2AggrsCondD).toArray, pCovMap)
  }

  private def broadcastCache(): (Array[TupleMap], PartialCovMap) = {
    val op1 = OperatorSerializationWrapper(this)

    val numKeysC = conf.getNumKeysContinuous
    val lenC = numKeysC.size()

    val id2AggrsCondC = numKeysC.zip(Range(1, 1 + lenC)).map { case (numKeys, idx) =>
      val op = op1
      val table = parentOperators(idx).execute().mapPartitions { iter =>
        val parser = new ContinuousOutputParserSimple(op.objectInspectors.get(idx), numKeys)
        iter.map(t => parser.parse(t))
      }.collect()

      // println("---------------BEGIN: continuous output " + idx + "-----------------------------")
      val hash = new TupleMap
      table.foreach { t =>
        // println("(" + t._1.mkString("[", ",", "]")  + ", (" + t._2.toString  + ", " + t._3.toString  + "), " + t._4.toString + ")")
        // id: (aggrs, cond)
        hash.put(t._4, new SrvTuple(t._1, t._2, t._3))
      }
      // println("---------------END: continuous output " + idx + "-----------------------------")

      hash
    }

    val numKeysD = conf.getNumKeysDiscrete
    val lenD = numKeysD.size()

    val rangeD = if (conf.isSimpleQuery) Range(1 + lenC, 1 + lenC + lenD) else Range(1 + lenC * 2, 1 + lenC * 2 + lenD)
    val id2AggrsCondD = numKeysD.zip(rangeD).map { case (numKeys, idx) =>
      val op = op1
      val table = parentOperators(idx).execute().mapPartitions { iter =>
        val parser = new DiscreteOutputParser(op.objectInspectors.get(idx), numKeys)
        iter.map(t => parser.parse(t))
      }.collect()

      // println("---------------BEGIN: discrete output " + idx + "-----------------------------")
      val hash = new TupleMap
      table.foreach { t =>
        // println("(" + t._1.mkString("[", ",", "]")  + ", (" + t._2.toString  + ", " + t._3.toString  + "), " + t._4.toString + ")")
        // id: (aggrs, cond)
        hash.put(t._4, new SrvTuple(t._1, t._2, t._3))
      }
      // println("---------------END: discrete output " + idx + "-----------------------------")

      hash
    }

    // val databaseName = AbmUtilities.ABM_CACHE_DB_NAME
    // conf.getCachedOutputs.foreach(SharkEnv.memoryMetadataManager.removeTable(databaseName, _))

    ((id2AggrsCondC ++ id2AggrsCondD).toArray, new PartialCovMap(new Array[Int](lenC)))
  }

  private def executeSimulation(rdd: RDD[_], srvs: Broadcast[Array[TupleMap]],
                                pCovMap: Broadcast[PartialCovMap]): RDD[_] = {
    val op = OperatorSerializationWrapper(this)
    rdd.mapPartitionsWithIndex { case (split, partition) =>
      op.logDebug("Started executing mapPartitions for operator: " + op)
      op.logDebug("Input object inspectors: " + op.objectInspectors)

      op.initializeOnSlave()
      val newPart = op.value.simulate(split, partition, srvs, pCovMap)

      op.logDebug("Finished executing mapPartitions for operator: " + op)

      newPart
    }
  }

  private def simulate(split: Int, iter: Iterator[_],
                       srvs: Broadcast[Array[TupleMap]], pCovMap: Broadcast[PartialCovMap]) = {
    val driver = new SrvTuple(null, null, null)
    val request = new Array[IntArrayList](1)
    request(0) = new IntArrayList()
    request(0).add(0)

    val sim = MCSimNode.createSimulationChain(driver, conf.getGbyIds, conf.getUdafTypes,
      conf.getAllPreds, srvs.value, pCovMap.value.getInnerGbyCovs, pCovMap.value.getInterGbyCovs,
      conf.isSimpleQuery)
    val parser = new FinalTupleParser(objectInspector)

    val reusedRow = new Array[Object](evals.length)
    iter.map { row =>
      driver.key = parser.parseKey(row)
      driver.range = parser.parseRange(row)
      samples.samples = sim.simulate(request)

      var i = 0
      while (i < evals.length) {
        reusedRow(i) = evals(i).evaluate(row)
        i += 1
      }
      reusedRow
    }
  }

}
