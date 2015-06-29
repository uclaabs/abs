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

package shark.execution.abm

import java.util.ArrayList

import it.unimi.dsi.fastutil.ints.IntArrayList
import org.apache.hadoop.hive.ql.abm.datatypes.{RangeList, ConditionsParser}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector => OI, StructObjectInspector}


/**
 * Created by kzeng on 6/18/14.
 */
class FinalTupleParser(_oi: OI) {

  private val oi = _oi.asInstanceOf[StructObjectInspector]
  private val field = {
    val fields = oi.getAllStructFieldRefs
    fields.get(fields.size() - 1)
  }
  private val parser = new ConditionsParser(field.getFieldObjectInspector)

  def parseKey(o: Any) = parser.inplaceParseKey(oi.getStructFieldData(o, field))

  def parseRange(o: Any) = parser.inplaceParseRange(oi.getStructFieldData(o, field))

}
