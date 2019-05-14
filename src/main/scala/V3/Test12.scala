
package V3


import java.io.{File, FileWriter}

import org.apache.spark.sql.SparkSession
import V3.Sdedup.getString
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

import scala.sys.process._
import org.apache.spark
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

object Test12 {

  case class SAM4(qname: String, flag: String, rname: String, pos: Long, others: String)


  def markDup(flag: Int) = {
    flag | 0x400
  }

  def markDups(sam: SAM4) = {
    val marked = markDup(sam.flag.toInt).toString
    val rebuild = SAM4(sam.qname, marked, sam.rname, sam.pos, sam.others)
    rebuild
  }

//  def parseDBG(str: String): String = {
//    val srcData = str
//    val spl = str.split("\\s+")
//
//    if (spl.length < 4) {
//      return "DBG less string: " + str
//    }
//
//    val qname = spl(0)
//    val flag = spl(1)
//    val rname = spl(2)
//
//
//    val pos1 = spl(3)
//    val pos = try {
//      pos1.toLong
//    } catch {
//      case e: Exception => {
//        e.printStackTrace()
//        0L
//      }
//    }
//    val others = spl.slice(4, spl.length)
//
//    val t1 = SAM4(qname, flag, rname, pos, others)
//    val t2 = getString(t1)
//
//    t2
//  }

  def parseToSAM(str: String): SAM4 = {
    val srcData = str
    val spl = str.split("\\s+")

    val qname = spl(0)
    val flag = spl(1)
    val rname = spl(2)
    val pos1 = spl(3)
    val pos = try {
      pos1.toLong
    } catch {
      case e: Exception => {
        e.printStackTrace()
        987654321L
      }
    }

    val others = spl.slice(4, spl.length).mkString("\t")

    SAM4(qname, flag, rname, pos, others)
  }

//  def getString(sam: SAM4) = {
//    val arr: Array[String] = Array(sam.qname, sam.flag, sam.rname, sam.pos.toString) ++ sam.others
//    val res = s"${arr.mkString("\t")}"
//    res
//  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: APP <SAM OUT PATH> <OUTPUT PATH>")
      System.exit(1)
    }


    val sparkConf = new SparkConf().setAppName("UNKNOWN-DEDUP")
    sparkConf.set("spark.executor.memoryOverhead", "8g")
    sparkConf.set("spark.driver.maxResultSize", "8g")



    val sc = new SparkContext(sparkConf)

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val filePath = args(0)
    val outPath = args(1)
    val partNum = args(2).toInt
    val stride = args(3).toInt
    val log = LogManager.getRootLogger

    val tc = new TimeChecker
    tc.checkTime("start")

    println("다음 파일에서 헤더를 읽습니다. " + filePath + "Output0.samsbl")
    val loadHeader = sc.textFile(filePath + "Output0.samsbl", partNum).filter(_.startsWith("@"))

    ppp.setDic(loadHeader.collect())
    val broadcastMap = sc.broadcast(ppp.getDict)
    val broadcastHeader = sc.broadcast(ppp.getHead)
    //    println(s"partitions1 : ${loadHeader.getNumPartitions}")

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
      // qname, dedup count
      res
    }
    // key1 (dedupKey, (qname, 0))
    key1.cache()

    val key = key1.filter(_._2._2 > 0).map(x => x._2).collectAsMap()

    tc.checkTime("Collect time ~")

    val bKey = sc.broadcast(key)

    //    DEBUG DEBUG DEBUG
    //    val total = key.values.foldLeft(0)(_ + _)
    //    println(s"deduped : ${total} / ${key1.count()}")
    tc.checkTime("key loaded")
    tc.printElapsedTime()
    println(s"다음 디렉토리에서 파일을 읽습니다. : ${filePath}")
    val samPathRegex = filePath + "*.sbl"

    val ablsolFileInputPath = s"/lustre/hadoop/user/${sc.sparkUser}/$filePath"
      .replaceAll("//", "/")
    val ablsolFileOutputPath = s"/lustre/hadoop/user/${sc.sparkUser}/$outPath"
      .replaceAll("//", "/")

    //    println(s"search path is ${ablsolFileInputPath}\n\n")
    val list = s"ls ${ablsolFileInputPath}".!!.split("\n").filter(_.endsWith("sbl"))
      .sortBy(x => x.slice(6, x.indexOf(".")).toInt)

    val len = list.length
    var lcnt = 0
    var cur = 0

    var totalRecords = 0L

    while (cur < len) {
      val ttc = new TimeChecker
      ttc.checkTime(s"LOOP START\n")
      val far = if ((cur + stride) > len) len
      else {
        val tmp = cur + stride
        tmp
      }
      val l = list.slice(cur, far).map(x => s"$filePath$x")
      //      println(s"files : ${l.head} ~ ${l.last}")
      println(s"다음 파일을 읽습니다. : ${l.mkString(",")}")
      println(s"from : $cur to : ${far - 1} max : ${len - 1}")

      val markedSamWithCounts = sc.textFile(l.mkString(","), partNum)
        .filter(!_.startsWith("@"))
        .map(x => parseToSAM(x))
        .map(x => (x.qname, (0, Seq(x))))
        .reduceByKey { (a, b) =>
          val s = a._2 ++ b._2
          (a._1, s)
        }
        .mapPartitions { xitr =>
          val bcMap = bKey.value
          val samIter = xitr.map { x =>
            val sam = x
            var cnt = x._2._1
            val col2 = if (!x._1.equals("*") && bcMap.isDefinedAt(x._1)) {
              val res = x._2._2.map(x => markDups(x))
              cnt = cnt + 1
              res
            } else x._2._2
            val res = (x._1, (cnt, col2))
            res
          }
          samIter
        }
      markedSamWithCounts.cache() //marked sam

      //      val markedCount = markedSamWithCounts.map(_._2._1).fold(0)(_+_)
      //      val thisRecords = markedSamWithCounts.map(_._1).count()
      //      totalRecords += thisRecords
      //      println("deduped : "+markedCount)
      //      println("Total Records : " + totalRecords)

      val samWithRnameKey = markedSamWithCounts.map { record =>
        //partitioning, write
        // ._1 qname
        record._2._2
      }.flatMap(x => x)
        .map { sam =>
          val bcMap = broadcastMap.value
          val idxedRname = bcMap.get(sam.rname).getOrElse(0) * 10000
          val dividedPos = (sam.pos % 1000).toInt
          val key = idxedRname + dividedPos
          val res = (key, sam)
          //
          //        val key1 = sam.rname
          //        val key2 = "%020d".format(sam.pos)
          //        val key3 = key1+key2
          //        val res2 = (key3, sam)

          //        println(s"info : ${sam.rname}, ${sam.pos}, ${key}")
          //        res2
          res
        }
      samWithRnameKey.cache()

      val voutPath = s"$outPath/loop${"%03d".format(lcnt)}"
      val rpart = new RangePartitioner(partNum, samWithRnameKey)
      val rdd2 = samWithRnameKey.partitionBy(rpart)
      val df = rdd2.map(x => (x._2.rname, x._2)).toDF
      val ss = df.write.partitionBy("_1").sortBy("pos").format("csv").option("sep", "\n").save(voutPath)


      //        .mapPartitions { Itr =>
      ////          val head = Seq(broadcastHeader.value.mkString("\n")).iterator
      //          val res = Itr.toSeq.sortBy(_.pos).iterator
      //          (res)
      //        }
      //
      //      rdd2.cache()
      //      val voutPath = s"$outPath/loop${"%03d".format(lcnt)}"
      //
      //      val rdd3 = rdd2.map(x => getString(x)).mapPartitions { itr =>
      //        val head = Seq(broadcastHeader.value.mkString("\n")).iterator
      //        (head ++ itr)
      //      }.saveAsTextFile(voutPath)

      //      val makeinfo = rdd2.mapPartitionsWithIndex { (idx, sam) =>
      //        if (!sam.isEmpty) {
      //          val rnames = sam.map(x => x.rname).toSeq.distinct.mkString(",")
      //          val partitionNo = "%05d".format(idx)
      //          val dirPath = s"${ablsolFileOutputPath}/loop${"%03d".format(lcnt)}".replaceAll("//", "/")
      //          val curPath = s"${dirPath}/part.info".replaceAll("//", "/")
      //          new File(dirPath).mkdirs()
      //          val partInfoFile = new FileWriter(curPath, true)
      //          partInfoFile.append(s"part-${partitionNo}\t${rnames}\n")
      //          partInfoFile.flush()
      //          partInfoFile.close()
      //        }
      //        sam
      //      }
      //
      //      makeinfo.count()
      samWithRnameKey.unpersist()

      ttc.checkTime()
      val minsec = ttc.getElapsedTimeAsMinSeconds
      println(s"${lcnt} 번째 작업에 ${minsec} 걸렸습니다. ")
      println("\n\n")
      lcnt += 1
      cur = far
    }
    tc.checkTime()
    val minsec = tc.getElapsedTimeAsMinSeconds
    println(s"총 소요시간은 ${minsec} 입니다.")
  }
}
