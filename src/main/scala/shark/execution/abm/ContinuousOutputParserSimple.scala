package org.apache.hadoop.hive.ql.abm.datatypes

import java.util

import it.unimi.dsi.fastutil.ints.IntArrayList
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector => OI, StructObjectInspector}

/**
 * Created by kzeng on 6/4/14.
 */
class ContinuousOutputParserSimple(_oi: OI, val numKeys: Int) {

  private val oi = _oi.asInstanceOf[StructObjectInspector]
  private val fields = oi.getAllStructFieldRefs
  // simple: keys, aggregates, condition, gby-id
  private val aggrParser = new ContinuousSrvParser(_oi, numKeys, fields.size() - 2)
  private val conditionField = fields.get(fields.size() - 2)
  private val conditionParser = new ConditionsParser(conditionField.getFieldObjectInspector)
  private val gbyIdField = fields.get(fields.size() - 1)
  private val gbyIdParser = new IdParser(gbyIdField.getFieldObjectInspector)

  def parse(o: Any): (Array[Double], IntArrayList, util.List[RangeList], Int) = {
    (aggrParser.parse(o),
      conditionParser.parseKey(oi.getStructFieldData(o, conditionField)),
      conditionParser.parseRange(oi.getStructFieldData(o, conditionField)),
      gbyIdParser.parse(oi.getStructFieldData(o, gbyIdField)))
  }

}
