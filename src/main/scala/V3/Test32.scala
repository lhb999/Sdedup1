package V3


import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.SparkSession
import V3.Sdedup.getString
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

import scala.sys.process._
import org.apache.spark
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}

object Test32 {
  case class SAM4(qname:String, flag:String, rname:String, pos:Long, others:Array[String])

  def checkFlag(a:Int, b:Int) = { (a&b) != 0 }
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

  def loadHeader(fileName:String, sc:SparkContext) = {

  }
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: APP <SAM OUT PATH> <OUTPUT PATH>")
      System.exit(1)
    }


    val sparkConf = new SparkConf().setAppName("UNKNOWN-DEDUP")
    sparkConf.set("spark.executor.memoryOverhead", "8g")
    sparkConf.set("spark.driver.maxResultSize", "24g")

    val sc = new SparkContext(sparkConf)


    val filePath = args(0)
    val outPath = args(1)
    val partNum = args(2).toInt
    val stride = args(3).toInt
    val log = LogManager.getRootLogger

    val tc = new TimeChecker
    tc.checkTime("start")

    println("load key file is "+ filePath + "Output0.samsbl")
    val loadHeader = sc.textFile(filePath + "Output0.samsbl", partNum).filter(_.startsWith("@"))
    ppp.setDic(loadHeader.collect())
    val broadcastMap = sc.broadcast(ppp.getDict)
    val broadcastHeader = sc.broadcast(ppp.getHead)
    //    println(s"partitions1 : ${loadHeader.getNumPartitions}")
    val ablsolFileInputPath = s"/lustre/hadoop/user/${sc.sparkUser}/$filePath"
      .replaceAll("//","/")
    val ablsolFileOutputPath = s"/lustre/hadoop/user/${sc.sparkUser}/$outPath"
      .replaceAll("//","/")


    val keyPathRegex = filePath + "*.key"
    val keyPathRegexx = ablsolFileInputPath

    val sst = (s"ls -l ${keyPathRegexx}".!!).split("\n").filter(_.endsWith(".key")).mkString("\n")
    println(sst)

    val accum = sc.longAccumulator("My Accumulator")

    //    val key2 = sc.textFile(keyPathRegex).flatMap(_.split("\n"))
    //    println(s"loaded key file lines : ${key2.count()}")
    val key1 = sc.textFile(keyPathRegex).flatMap(_.split("\n"))
      .map { x =>
        val spl = x.split("\t")
        val qname = spl(0)
        val dedupKey = spl(1).toLong
        val un1 = spl(2).toInt
        val un2 = spl(3).toInt
        val un = un1 + un2
        (dedupKey, (Seq(qname), 0, un))
      }.filter(x => x._2._3 == 0)
      .map(x => (x._1, (x._2._1, x._2._2)))
      .reduceByKey { (a, b) =>
        val v = a._2 + b._2 + 1
        val res1 = a._1 ++ b._1
        val res = (res1, v)
        res
      }.filter(x => x._2._2 > 0)
    // key1 (dedupKey, (qname, 0))

    val key = key1.filter(_._2._2 > 0)

    key.cache()
//    val tmp = key.map(x => (x._1, x._2._1)).collect().foreach{ record =>
//      val  k = record._1
//      val v = record._2.mkString(", ")
//      println(s"${k}\t${v}")
//    }
    val tmp = key1.map(x => (x._1, x._2._1)).collect().foreach{ record =>
//      val  k = record._1
      val v = record._2.map(x => s"${x}\t${record._1}").foreach(println(_))
    }

    //    println(key.map(_._1).collect().mkString("\n"))
    val testKey = key.map(x => x._2._2).fold(0)(_+_)
    println("deduped counts 1: "+testKey)

    tc.checkTime("key loaded")
    tc.printElapsedTime()

    println(s"READ PATH IS ${filePath}")
    val samPathRegex = filePath + "*.sbl"


    println(s"deduped qname 1 : ${accum.value}")
    //      println(s"deduped qname 2 : ${totalCounts2} / ${totalCounts}")
    //      println(s" ===> LOOP : ${lcnt} INTERVAL : ${minsec}<===")

    tc.checkTime()
    val minsec = tc.getElapsedTimeAsMinSeconds
    println(s" ===> TOTAL ELAPSED TIME : ${minsec}<===")
  }
}



