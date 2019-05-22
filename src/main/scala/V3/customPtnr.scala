package V3

import org.apache.spark.Partitioner

import scala.collection.mutable.ArrayBuffer

case class rKey(rname:String, pos:Long)
case class acMapVal(idx:Int, rPartNum:Int, accVal:Int)

class customPtnr(rmap: Seq[(String, Int)], override val numPartitions: Int) extends Partitioner {
  def getMapSize(rmap:  Map[String, Int]) = {
    val mapLen = rmap.map(_._2).foldLeft(0)(_+_)
    mapLen
  }
//  def adjustMapSize(rmap: Map[String, Int]): Map[String, Int] = {
//    val max = rmap.maxBy(_._2)
//    val newv = max._2 - 1
//    val res = rmap.updated(max._1, newv)
//    if(getMapSize(res) > numPartitions) adjustMapSize(res)
//    else res
//  }
  def buildAcRnameMap(rmap: Map[String, Int]): Map[String, acMapVal] = {
//    val map = Map[String, acMapVal]()
    val map = new ArrayBuffer[(String, acMapVal)]()
    val tmpMap = rmap.toSeq.sortBy(_._1)
    var accVal = 0
    var idx = 0

    tmpMap.foreach{ record =>
      val tmpRecord = acMapVal(idx, record._2, accVal)
      idx += 1
      accVal += record._2
//      map.+((record._1, accVal))
      map.+=((record._1, tmpRecord))
    }
    println("Builded Map")
    println(map.mkString("\n"))

    map.toMap
  }
  val parsedMap = rmap.toMap
//  val initMap = adjustMapSize(parsedMap)
  val rnameMap = buildAcRnameMap(parsedMap)

  def getPartition(key: Any): Int = key match {
    case s: Int => {
      s % numPartitions
    }
    case k : rKey => {
      val t = rnameMap.get(k.rname).get
      val tk = (k.pos % t.rPartNum)
      val tkk = tk + t.accVal

      tkk.toInt
    }
    case _ =>
      0
  }
}