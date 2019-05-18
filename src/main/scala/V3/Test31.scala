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

object Test31 {
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

  def loadHeader(fileName:String, sc:SparkContext) = {

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
    println(s"partitions1 : ${loadHeader.getNumPartitions}")

    val keyPathRegex = filePath + "*.key"
    val key1 = sc.textFile(keyPathRegex).flatMap(_.split("\n"))
      .map { x =>
        val spl = x.split("\t")
        val qname = spl(0)
        val dedupKey = spl(1)
        (dedupKey, (qname, 0))
      }.reduceByKey { (a, b) =>
      val v = a._2 + b._2 + 1
      val res = (a._1, v)
      res
    }
    // key1 (dedupKey, (qname, 0))
    val key = key1.filter(_._2._2 > 0).map(x => x._2).collectAsMap()
    val bKey = sc.broadcast(key)

    tc.checkTime("key loaded")
    tc.printElapsedTime()

    println(s"READ PATH IS ${filePath}")
    val samPathRegex = filePath + "*.sbl"

    val ablsolFileInputPath = s"/lustre/hadoop/user/${sc.sparkUser}/$filePath"
      .replaceAll("//","/")
    val ablsolFileOutputPath = s"/lustre/hadoop/user/${sc.sparkUser}/$outPath"
      .replaceAll("//","/")

    println(s"search path is ${ablsolFileInputPath}\n\n")
    val list = s"ls ${ablsolFileInputPath}".!!.split("\n")
    val samList = list.filter(_.endsWith("sbl"))
      .sortBy(x => x.slice(6, x.indexOf(".")).toInt)

    val rnameList = list.filter(_.endsWith("rname"))
      .sortBy(x => x.slice(6, x.indexOf(".")).toInt)

    val len = samList.length
    var lcnt = 0
    var cur = 0

    var totalRecords = 0L

    val hashPtnr = new HashPartitioner(partNum)

    def get_num_keys_rname(value:Int) = {
      val res =
        if(value == 0 ) 1
        else value

      res
    }
    while(cur < len) {
      val ttc = new TimeChecker
      ttc.checkTime(s"LOOP START\n")
      val far = if((cur + stride) > len) len
      else {
        val tmp = cur + stride
        tmp
      }
      val currentSamfileList = samList.slice(cur, far).map(x => s"$filePath$x")
      val currentRnameList = rnameList.slice(cur, far).map(x => s"$filePath$x")

      //      println(s"files : ${l.head} ~ ${l.last}")
      println(s"files : ${currentSamfileList.mkString(",")}")
      println(s"from : $cur to : ${far-1} max : ${len-1}")

      val rnameCounts = sc.textFile(currentRnameList.mkString(","), partNum)
        .map{ rnameWithCount =>
          val spl = rnameWithCount.split("\t")
          val k = spl(0)
          val v = spl(1).toInt
          (k,v)
        }.reduceByKey(_+_)

      val total_num_samrecords = rnameCounts.map(_._2).fold(0)(_+_)
      val expected_num_samrecords_partition = total_num_samrecords /partNum
      val collected = rnameCounts.collectAsMap()
      val rnameCountsMapBC =
        sc.broadcast(collected
          .map(x => (x._1, get_num_keys_rname(x._2/expected_num_samrecords_partition))))

      println("expected_num_samrecords_partition : "+expected_num_samrecords_partition)
      println(rnameCountsMapBC.value.mkString("\n"))


      val markedSamWithCounts = sc.textFile(currentSamfileList.mkString(","), partNum)
        .filter(!_.startsWith("@"))
        .map(x => parseToSAM(x))
        .map{sam =>
          val bcMap = broadcastMap.value
          val rname_code = bcMap.get(sam.rname).getOrElse(0) * 10000
          val mod = rnameCountsMapBC.value.getOrElse(sam.rname,1)
          val pos_code = (sam.pos % mod)
          val rname_key = rname_code + pos_code
          (rname_key,  Seq(sam))
        }.partitionBy(hashPtnr).flatMap(x => x._2)
        .mapPartitionsWithIndex{ (idx, sam) =>
          val iter = sam.toSeq.sortBy(_.pos).iterator
          iter

        }
//
      markedSamWithCounts.cache()

      val voutPath = s"$outPath/loop${"%03d".format(lcnt)}"
      markedSamWithCounts.map(x => getString(x)).saveAsTextFile(voutPath)

      markedSamWithCounts.mapPartitionsWithIndex{ (idx, sam) =>
        if (!sam.isEmpty) {
          val rnames = sam.map(x => x.rname).toSeq.distinct.mkString(",")
          val partitionNo = "%05d".format(idx)
          val dirPath = s"${ablsolFileOutputPath}/loop${"%03d".format(lcnt)}".replaceAll("//", "/")
          val curPath = s"${dirPath}/part.info".replaceAll("//", "/")
          new File(dirPath).mkdirs()
          val partInfoFile = new FileWriter(curPath, true)
          partInfoFile.append(s"part-${partitionNo}\t${rnames}\n")
          partInfoFile.flush()
          partInfoFile.close()
        }
        sam
      }.count()
      markedSamWithCounts.unpersist()
      ttc.checkTime()
      val minsec = ttc.getElapsedTimeAsMinSeconds
      println(s" ===> LOOP : ${lcnt} INTERVAL : ${minsec}<===")
      println("\n\n")
      lcnt+=1
      cur = far
    }
    tc.checkTime()
    val minsec = tc.getElapsedTimeAsMinSeconds
    println(s" ===> TOTAL ELAPSED TIME : ${minsec}<===")
  }
}



