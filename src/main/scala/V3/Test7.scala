package V3


import java.io.{File, FileWriter}

import org.apache.spark.sql.SparkSession
import V3.Sdedup.getString
import V3.{SAM4, String2SAM}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

import scala.sys.process._
import org.apache.spark
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Test7 {
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
    val hashPtnr = new spark.HashPartitioner(partNum)
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
        val key = spl(1)
        (key, (qname, 0))
      }.reduceByKey { (a, b) =>
      val v = a._2 + b._2 + 1
      val res = (a._1, v)
      // qname, dedup count
      res
    }
    key1.cache()

    val key = key1.filter(_._2._2 > 0).map(x => x._2).collectAsMap()

    tc.checkTime("Collect time ~")

    val bKey = sc.broadcast(key)

    val total = key.values.foldLeft(0)(_+_)
    println(s"deduped : ${total} / ${key1.count()}")

    tc.checkTime("key loaded")
    tc.printElapsedTime()

    println(s"READ PATH IS ${filePath}")
    val samPathRegex = filePath + "*.sbl"

    val ablsolFileInputPath = s"/lustre/hadoop/user/${sc.sparkUser}/$filePath".replaceAll("//","/")
    val ablsolFileOutputPath = s"/lustre/hadoop/user/${sc.sparkUser}/$outPath".replaceAll("//","/")

    println(s"search path is ${ablsolFileInputPath}\n\n")
    val list = s"ls ${ablsolFileInputPath}".!!.split("\n").filter(_.endsWith("sbl")).sortBy(x => x.slice(6, x.indexOf(".")).toInt)
    //    println(s"list : ${list.mkString("\n")}")

    val len = list.length
    var lcnt = 0
    var cur = 0

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    ssc.textFileStream(s"${filePath}")

    while(cur < len) {
      val ttc = new TimeChecker
      ttc.checkTime(s"LOOP START\n")
      val far = if((cur + stride) > len) len - 1
      else {
        val tmp = cur + stride
        tmp
      }
      val l = list.slice(cur, far).map(x => s"$filePath$x")
      println(s"files : ${l.head} ~ ${l.last}")
      println(s"from : $cur to : $far max : $len")

      val rdd1 = sc.textFile(l.mkString(","), partNum).filter(!_.startsWith("@")).mapPartitions{ xitr =>
        val bcMap = bKey.value
        val samIter = xitr.map{ x =>
          val sam = String2SAM.parseToSAM(x)
          if(!sam.rname.equals("*") && bcMap.isDefinedAt(sam.qname)) String2SAM.markDups(sam)
          sam
        }
        samIter
      }.map{ sam =>
        val bcMap = broadcastMap.value
        val idxedRname = bcMap.get(sam.rname).getOrElse(0) * 1000
        val dividedPos = (sam.pos / 100000000).toInt
        val key = idxedRname + dividedPos
        val res = (key, sam)
        println(s"info : ${sam.rname}, ${sam.pos}, ${key}")
        res
      }.partitionBy(hashPtnr).map(_._2)
      rdd1.cache()



      val voutPath = s"$outPath/loop${"%03d".format(lcnt)}"
      println(s"save dir is ${voutPath}")

      rdd1.saveAsTextFile(voutPath)

      val makeinfo = rdd1.mapPartitionsWithIndex{ (idx, sam) =>
        if(!sam.isEmpty){
          val rnames = sam.map(x => x.rname).toSeq.head
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
      }

      makeinfo.count()


      rdd1.unpersist()

      ttc.checkTime(s" ===> LOOP : ${lcnt} INTERVAL : <===")
      ttc.printInterval()
      println("\n\n")
      lcnt+=1
      cur = far
    }
    tc.checkTime()
    tc.printElapsedTime()
  }
}

