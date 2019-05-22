package V3


import java.io.{File, FileWriter}

import org.apache.log4j.LogManager
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.sys.process._

object keyTest {
  case class SAM4(qname:String, flag:String, rname:String, pos:Long, others:Array[String])
  def markDup(flag:Int) = { flag | 0x400 }
  def markDups(sam:SAM4) = {
    val marked = markDup(sam.flag.toInt).toString
    val rebuild = SAM4(sam.qname, marked, sam.rname, sam.pos, sam.others)
    rebuild
  }
  def parseDBG(str:String) : String = {
    val srcData = str
    val spl = str.split("\\s+")

    if(spl.length < 4) {
      return "DBG less string: "+str
    }

    val qname = spl(0)
    val flag = spl(1)
    val rname = spl(2)


    val pos1 = spl(3)
    val pos = try {
      pos1.toLong
    } catch {
      case e:Exception => {
        e.printStackTrace()
        0L
      }
    }
    val others = spl.slice(4, spl.length)

    val t1 = SAM4(qname, flag, rname, pos, others)
    val t2 = getString(t1)

    t2
  }

  def parseToSAM(str:String) : SAM4 = {
    val srcData = str
    val spl = str.split("\\s+")

    val qname = spl(0)
    val flag = spl(1)
    val rname = spl(2)
    val pos1 = spl(3)
    val pos = try {
      pos1.toLong
    } catch {
      case e:Exception => {
        e.printStackTrace()
        987654321L
      }
    }

    val others = spl.slice(4, spl.length)

    SAM4(qname, flag, rname, pos, others)
  }

  def getString(sam:SAM4) = {
    val arr:Array[String] = Array(sam.qname, sam.flag, sam.rname, sam.pos.toString) ++ sam.others
    val res = s"${arr.mkString("\t")}"
    res
  }

  def slashRmver(path:String ) = {
    val improvedPath =
      if(path.endsWith("/")) path.substring(0, path.length-1)
      else path
    improvedPath
  }
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: APP <SAM OUT PATH> <OUTPUT PATH>")
      System.exit(1)
    }


    val sparkConf = new SparkConf().setAppName("UNKNOWN-DEDUP")
    sparkConf.set("spark.executor.memoryOverhead", "8g")
    sparkConf.set("spark.driver.maxResultSize", "8g")

    val sc = new SparkContext(sparkConf)


    val filePath = slashRmver(args(0))
    val outPath = slashRmver(args(1))
    val partNum = args(2).toInt
    val stride = args(3).toInt
    val log = LogManager.getRootLogger

    val tc = new TimeChecker
    tc.checkTime("start")

    val accum = sc.longAccumulator("My Accumulator")

    println("다음 파일에서 헤더를 읽습니다. "+ filePath + "/Output0.samsbl")
    val loadHeader = sc.textFile(filePath + "/Output0.samsbl", partNum).filter(_.startsWith("@"))

    ppp.setDic(loadHeader.collect())
    println("헤더가 로드 완료.")
    val broadcastMap = sc.broadcast(ppp.getDict)
    val broadcastHeader = sc.broadcast(ppp.getHead)
    //    println(s"partitions1 : ${loadHeader.getNumPartitions}")

    val keyPathRegex = filePath + "/*.key"
    println("다음 경로에서 중복제거 키 파일을 읽습니다. "+ keyPathRegex)


    val key3 = sc.textFile(keyPathRegex).flatMap(_.split("\n"))
      .map { x =>
        val spl = x.split("\t")
        val qname = spl(0)
        val dedupKey = spl(1)
        (dedupKey, (qname, 0))
      }.reduceByKey { (a, b) =>
      val v = a._2 + b._2 + 1
      val res = (a._1, v)
      res
    }.collectAsMap()
    key3
    tc.checkTime("key loaded 1")

    val key1 = sc.textFile(keyPathRegex).flatMap(_.split("\n"))
      .map { x =>
        val spl = x.split("\t")
        val qname = spl(0)
        val dedupKey = spl(1).toLong
        (dedupKey, (Seq(qname), 0))
      }
      .reduceByKey { (a, b) =>
        val v = a._2 + b._2 + 1
        val res1 = a._1 ++ b._1
        val res = (res1, v)
        res
      }.filter(x => x._2._2 > 0)

    tc.checkTime("key loaded 2")

    val key = key1.flatMap(x => (x._2._1)).map(x => (x, 0)).collectAsMap()
    println("키 로드 완료.")
    val bKey = sc.broadcast(key)

    tc.checkTime("key loaded")
    tc.printElapsedTime()

  }
}



