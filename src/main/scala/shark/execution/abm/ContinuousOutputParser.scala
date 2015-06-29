/*
 * Copyright (C) 2015 The Regents of The University California.
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
