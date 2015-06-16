package org.apache.hadoop.hive.ql.abm.datatypes

import java.util.{List => JList}

import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector => OI, StructField, StructObjectInspector}
import org.apache.hadoop.hive.ql.abm.rewrite.UdafType
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import scala.collection.mutable.ArrayBuffer
import shark.execution.serialization.SerializableWritable
import org.apache.hadoop.io.Writable

/**
 * Created by kzeng on 6/6/14.
 */
class InputParser(_oi: OI, val numKeys: Int, val aggrTypes: JList[UdafType],
                  val iter: Iterator[_], val gby: Int) extends Iterator[InputTuple] {

  private val oi = _oi.asInstanceOf[StructObjectInspector]
  private val fields = oi.getAllStructFieldRefs
  // keys, vals, tid
  private val keyParser = new GroupByKeyParser(_oi, numKeys)
  private val (valParser: Array[(StructField, DoubleParser)], buffer: Array[Double]) = {
    val buf = new Array[Double](aggrTypes.size())
    val parsers = new ArrayBuffer[(StructField, DoubleParser)]()

    var i: Int = 0
    var j: Int = numKeys
    while (i < aggrTypes.size()) {
      if (UdafType.COUNT != aggrTypes.get(i)) {
        val field = fields.get(j)
        parsers += Tuple2(field, DoubleParser(field.getFieldObjectInspector, buf, i))
        j += 1
      } else {
        buf(i) = 1
      }
      i += 1
    }
    (parsers.toArray, buf)
  }
  val tuple = new InputTuple(keyParser.buffer, buffer, 0)

  def hasNext = iter.hasNext

  def next(): InputTuple = {
    val o = iter.next()
    keyParser.inplaceParse(o)
    valParser.foreach(p => p._2.inplaceParse(oi.getStructFieldData(o, p._1)))
    tuple.tid = 1
    tuple
  }

}

class InputTuple(override val _1: ArrayBuffer[SerializableWritable[Writable]], override val _2: Array[Double], var tid: Int)
  extends Tuple2[ArrayBuffer[SerializableWritable[Writable]], Array[Double]](_1, _2)

abstract class DoubleParser(val buf: Array[Double], val index: Int) {
  def inplaceParse(o: Any): Unit
}

class Byte2DoubleParser(_oi: OI, override val buf: Array[Double], override val index: Int)
  extends DoubleParser(buf, index) {
  private val oi = _oi.asInstanceOf[ByteObjectInspector]
  override def inplaceParse(o: Any): Unit = {
    buf(index) = oi.get(o)
  }
}

class Short2DoubleParser(_oi: OI, override val buf: Array[Double], override val index: Int)
  extends DoubleParser(buf, index) {
  private val oi = _oi.asInstanceOf[ShortObjectInspector]
  override def inplaceParse(o: Any): Unit = {
    buf(index) = oi.get(o)
  }
}

class Int2DoubleParser(_oi: OI, override val buf: Array[Double], override val index: Int)
  extends DoubleParser(buf, index) {
  private val oi = _oi.asInstanceOf[IntObjectInspector]
  override def inplaceParse(o: Any): Unit = {
    buf(index) = oi.get(o)
  }

}

class Long2DoubleParser(_oi: OI, override val buf: Array[Double], override val index: Int)
  extends DoubleParser(buf, index) {
  private val oi = _oi.asInstanceOf[LongObjectInspector]
  override def inplaceParse(o: Any): Unit = {
    buf(index) = oi.get(o)
  }
}

class Float2DoubleParser(_oi: OI, override val buf: Array[Double], override val index: Int)
  extends DoubleParser(buf, index) {
  private val oi = _oi.asInstanceOf[FloatObjectInspector]
  override def inplaceParse(o: Any): Unit = {
    buf(index) = oi.get(o)
  }
}

class Double2DoubleParser(_oi: OI, override val buf: Array[Double], override val index: Int)
  extends DoubleParser(buf, index) {
  private val oi = _oi.asInstanceOf[DoubleObjectInspector]
  override def inplaceParse(o: Any): Unit = {
    buf(index) = oi.get(o)
  }
}

class String2DoubleParser(_oi: OI, override val buf: Array[Double], override val index: Int)
  extends DoubleParser(buf, index) {
  private val oi = _oi.asInstanceOf[StringObjectInspector]
  override def inplaceParse(o: Any): Unit = {
    buf(index) = java.lang.Double.parseDouble(oi.getPrimitiveJavaObject())
  }
}

object DoubleParser {
  def apply(oi: OI, buf: Array[Double], index: Int): DoubleParser = {
    if (oi.isInstanceOf[ByteObjectInspector]) {
      new Byte2DoubleParser(oi, buf, index)
    } else if (oi.isInstanceOf[ShortObjectInspector]) {
      new Short2DoubleParser(oi, buf, index)
    } else if (oi.isInstanceOf[IntObjectInspector]) {
      new Int2DoubleParser(oi, buf, index)
    } else if (oi.isInstanceOf[LongObjectInspector]) {
      new Long2DoubleParser(oi, buf, index)
    } else if (oi.isInstanceOf[FloatObjectInspector]) {
      new Float2DoubleParser(oi, buf, index)
    } else if (oi.isInstanceOf[DoubleObjectInspector]) {
      new Double2DoubleParser(oi, buf, index)
    } else if (oi.isInstanceOf[StringObjectInspector]) {
      new String2DoubleParser(oi, buf, index)
    } else {
      throw new IllegalArgumentException("Invalid object inspector type " + oi.getClass)
    }
  }
}

object InputParser {

  implicit object TidOrdering extends Ordering[InputParser] {

    override def compare(x: InputParser, y: InputParser): Int = x.tuple.tid - y.tuple.tid

  }

}