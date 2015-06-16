package org.apache.hadoop.hive.ql.abm.datatypes

import java.util

import it.unimi.dsi.fastutil.ints.IntArrayList
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector => OI, StructObjectInspector}
import shark.execution.serialization.SerializableWritable
import com.googlecode.javaewah.EWAHCompressedBitmap
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.io.Writable

/**
 * Created by kzeng on 6/4/14.
 */
class ContinuousOutputParser(_oi: OI, val numKeys: Int) {

  private val oi = _oi.asInstanceOf[StructObjectInspector]
  private val fields = oi.getAllStructFieldRefs
  // keys, aggregates, lineage, condition, gby-id
  private val keyParser = new GroupByKeyParser(_oi, numKeys)
  private val aggrParser = new ContinuousSrvParser(_oi, numKeys, fields.size() - 3)
  private val lineageField = fields.get(fields.size() - 3)
  private val lineageParser = new LineagesParser(lineageField.getFieldObjectInspector)
  private val conditionField = fields.get(fields.size() - 2)
  private val conditionParser = new ConditionsParser(conditionField.getFieldObjectInspector)
  private val gbyIdField = fields.get(fields.size() - 1)
  private val gbyIdParser = new IdParser(gbyIdField.getFieldObjectInspector)

  def parse(o: Any): (ArrayBuffer[SerializableWritable[Writable]], Array[Double], Array[EWAHCompressedBitmap], IntArrayList, util.List[RangeList], Int) = {
    (keyParser.parse(o), aggrParser.parse(o),
      lineageParser.parse(oi.getStructFieldData(o, lineageField)),
      conditionParser.parseKey(oi.getStructFieldData(o, conditionField)),
      conditionParser.parseRange(oi.getStructFieldData(o, conditionField)),
      gbyIdParser.parse(oi.getStructFieldData(o, gbyIdField)))
  }

}
