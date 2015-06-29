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

/**
 * Created by kzeng on 6/4/14.
 */
class DiscreteOutputParser(_oi: OI, val numKeys: Int) {

  private val oi = _oi.asInstanceOf[StructObjectInspector]
  private val fields = oi.getAllStructFieldRefs
  // aggregates, condition, gby-id
  private val aggrParser = new DiscreteSrvParser(_oi, numKeys, fields.size() - 2)
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
