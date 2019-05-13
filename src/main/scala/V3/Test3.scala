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
import org.apache.spark.{SparkConf, SparkContext}

object Test3 {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: APP <SAM OUT PATH> <OUTPUT PATH>")
      System.exit(1)
    }


    val sparkConf = new SparkConf().setAppName("UNKNOWN-DEDUP")
    sparkConf.set("spark.executor.memoryOverhead", "8g")
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

    val keyPathRegex = filePath + "*.key"
    val key = sc.textFile(keyPathRegex, partNum).flatMap(_.split("\n"))
      .map { x =>
        val spl = x.split("\t")
        val qname = spl(0)
        val key = spl(1)
        (key, (qname, 0))
      }.reduceByKey { (a, b) =>
      val v = a._2 + b._2 + 10
      val res = (a._1, v)
      // qname, dedup?
      res
    }.filter(_._2._2 > 2).map(x => (x._1, (x._2._2, Seq.empty[SAM4])))
    key.cache()

    tc.checkTime("key loaded")
    tc.printElapsedTime()

    println(s"READ PATH IS ${filePath}")
    val samPathRegex = filePath + "*.sbl"

    val absolPath = s"/lustre/hadoop/user/${sc.sparkUser}/$filePath"

    println(s"search path is ${absolPath}")
    val list = s"ls ${absolPath}".!!.split("\n").filter(_.endsWith("sbl")).sortBy(x => x.slice(6, x.indexOf(".")).toInt)
//    println(s"list : ${list.mkString("\n")}")

    val len = list.length
    var lcnt = 0
    var cur = 0

//    val l = list.slice(0, 5).map(x => s"$filePath$x")
//    println(s"files : ${l.mkString(",")}")

    while(cur < len) {
      val ttc = new TimeChecker
      ttc.checkTime(s"LOOP START\n")
      val far = if((cur + stride) > len) len - 1
                else {
                  val tmp = cur + stride
                  tmp
                }

//      println(s"cur : ${cur}, far : ${far}")
      val l = list.slice(cur, far).map(x => s"$filePath$x")
      println(s"files : ${l.mkString(",")}")
      println(s"from : $cur to : $far max : $len")

      val rdd = sc.textFile(l.mkString(",")).filter(!_.startsWith("@")).map{ x =>
        val tmp = String2SAM.parseToSAM(x)
        (tmp.qname, (0,Seq(tmp)))
      }
      .union(key).reduceByKey{ (a,b) =>
        val v = a._1 + b._1
        val res = if(v >= 10)(a._2++b._2).map(x => String2SAM.markDups(x))
        else (a._2++b._2)
        (v, res)
      }.map(_._2._2)
        .map { x =>
          val paired = x.map { sam =>
            val bcMap = broadcastMap.value
            val idxedRname = bcMap.get(sam.rname).getOrElse(0) * 1000
            val dividedPos = (sam.pos / 100000000).toInt
            val key = idxedRname + dividedPos
            val res = (key, sam)
            res
          }
          paired
        }
        .flatMap(x => x)
        .partitionBy(hashPtnr)
        .map(x => x._2)
        .mapPartitions { samIter =>
          val head = Seq(broadcastHeader.value.mkString("\n")).iterator
          val sorted = samIter.toSeq.sortBy(_.pos).map(x => getString(x)).iterator
          val res = if (sorted.isEmpty) sorted
          else head ++ sorted
          res
        }
      rdd.cache()

      val voutPath = s"$outPath/loop${"%03d".format(lcnt)}"
      println(s"save dir is ${voutPath}")
      rdd.foreachPartition( x =>
        x
      )
      rdd.saveAsTextFile(voutPath)
      ttc.checkTime(s"LOOP : ${lcnt} INTERVAL : ")
      ttc.printInterval()
      lcnt+=1
      cur = far
      rdd.unpersist()
      //    val deduped = key.map(x => x._2._1).fold(0)(_+_) / 10
      //    val recordNumb = key.count()
      //    println(s" $deduped / $recordNumb")
    }
    tc.checkTime()
    tc.printElapsedTime()

  }
}

