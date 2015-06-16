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
