package org.apache.spark.rdd

import java.util.{ArrayList => JList, HashMap => JHashMap}

import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.hadoop.hive.ql.abm.datatypes.InputParser
import shark.execution.serialization.{SerializableWritable, OperatorSerializationWrapper}
import shark.execution.SelectOperator
import scala.collection.mutable.{ArrayBuffer, Buffer, PriorityQueue}
import it.unimi.dsi.fastutil.ints.IntArrayList
import org.apache.spark.broadcast.Broadcast
import org.apache.hadoop.io.Writable
import com.googlecode.javaewah.EWAHCompressedBitmap


/**
 * Created by kzeng on 6/6/14.
 */
class ZippedLookupRDD(val op: OperatorSerializationWrapper[SelectOperator],
                val lookup: Broadcast[Buffer[JHashMap[ArrayBuffer[SerializableWritable[Writable]], (Array[EWAHCompressedBitmap], Int)]]],
                sc: SparkContext, rdds: Array[RDD[_]]) extends ZippedPartitionsBaseRDD[ZippedTuple](sc, rdds) {

  override def compute(split: Partition, context: TaskContext): Iterator[ZippedTuple] = {
    val partitions: Seq[Partition] = split.asInstanceOf[ZippedPartitionsPartition].partitions

    new Iterator[ZippedTuple] {
      private val parsers = {
        val numKeys = op.conf.getNumKeysContinuous
        val ois = op.objectInspectors.slice(1, 1 + numKeys.size())
        val aggrTypes = op.conf.getAggrTypesContinuous

        val pars = new ArrayBuffer[InputParser](ois.size)
        var i = 0
        while (i < ois.size) {
          val iter = rdds(i).iterator(partitions(i), context)
          if (iter.hasNext) {
            pars += new InputParser(ois(i), numKeys.get(i), aggrTypes.get(i), iter, i)
          }
          i += 1
        }
        pars
      }
      private val heap = new PriorityQueue[InputParser]()
      private var active = parsers.size

      private val buffer = new ZippedTuple(0, new IntArrayList(active), new IntArrayList(active),
        new JList[Array[EWAHCompressedBitmap]](active), new JList[Array[Double]](active))

      override def hasNext() = (active != 0)

      override def next(): ZippedTuple = {
        if (!parsers.isEmpty) {
          parsers.foreach(_.next)
          heap ++= parsers
          parsers.clear()
        }

        buffer.tid = heap.head.tuple.tid
        buffer.gbys.clear()
        buffer.groupIds.clear()
        buffer.lineages.clear()
        buffer.vals.clear()
        while (!heap.isEmpty && heap.head.tuple.tid == buffer.tid) {
          val cur = heap.dequeue()
          if (cur.hasNext) {
            parsers += cur
          } else {
            active -= 1
          }
          buffer.gbys.add(cur.gby)
          val output = lookup.value(cur.gby).get(cur.tuple._1)
          buffer.groupIds.add(output._2)
          buffer.lineages.add(output._1)
          buffer.vals.add(cur.tuple._2)
        }

        buffer
      }
    }
  }

}

class ZippedTuple(var tid: Int, val gbys: IntArrayList, val groupIds: IntArrayList,
                  val lineages: JList[Array[EWAHCompressedBitmap]], val vals: JList[Array[Double]])