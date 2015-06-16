package org.apache.hadoop.hive.ql.abm.datatypes

import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector => OI, ObjectInspectorUtils => OIUtils}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.{ObjectInspectorCopyOption => CopyOption}
import org.apache.hadoop.hive.serde2.objectinspector.{StructField, StructObjectInspector}
import org.apache.hadoop.io.Writable
import shark.execution.serialization.SerializableWritable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by kzeng on 6/3/14.
 */
class GroupByKeyParser(_oi: OI, val numKeys: Int) {

  private val oi = _oi.asInstanceOf[StructObjectInspector]
  private val keys: ArrayBuffer[(StructField, OI)] = {
    val fields = oi.getAllStructFieldRefs
    val buf = new ArrayBuffer[(StructField, OI)]()
    (0 until numKeys).foreach(i => {
      val field = fields.get(i)
      val oi = field.getFieldObjectInspector
      buf += Tuple2(field, oi)
    })
    buf
  }
  val buffer = {
    val buf = new ArrayBuffer[SerializableWritable[Writable]]
    (0 until numKeys).foreach(i => {
      buf += new SerializableWritable[Writable](null)
    })
    buf
  }

  def parse(o: Any): ArrayBuffer[SerializableWritable[Writable]] = keys.map { key =>
    new SerializableWritable(
      OIUtils.copyToStandardObjectForShark(oi.getStructFieldData(o, key._1), key._2, CopyOption.WRITABLE).asInstanceOf[Writable]
    )
  }

  def inplaceParse(o: Any): ArrayBuffer[SerializableWritable[Writable]] = {
    var i = 0
    while (i < buffer.size) {
      val key = keys(i)
      buffer(i).t = OIUtils.copyToStandardObjectForShark(oi.getStructFieldData(o, key._1), key._2, CopyOption.WRITABLE).asInstanceOf[Writable]
      i += 1
    }
    buffer
  }

}
