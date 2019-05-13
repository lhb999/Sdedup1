package V3


import java.io.{File, FileWriter}

import org.apache.spark.sql.SparkSession
import V3.Sdedup.getString
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

import scala.sys.process._
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

object Test10 {
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

    //    DEBUG DEBUG DEBUG
    //    val total = key.values.foldLeft(0)(_+_)
    //    println(s"deduped : ${total} / ${key1.count()}")

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

    while(cur < len) {
      val ttc = new TimeChecker
      ttc.checkTime(s"LOOP START\n")
      val far = if((cur + stride) > len) len
      else {
        val tmp = cur + stride
        tmp
      }
      val l = list.slice(cur, far).map(x => s"$filePath$x")
      //      println(s"files : ${l.head} ~ ${l.last}")
      println(s"files : ${l.mkString(",")}")
      println(s"from : $cur to : ${far-1} max : ${len-1}")

      val rdd1 = sc.textFile(l.mkString(","), partNum).filter(!_.startsWith("@")).mapPartitions{ xitr =>
        val bcMap = bKey.value
        val samIter = xitr.map{ x =>
          val sam = parseToSAM(x)
          if(!sam.rname.equals("*") && bcMap.isDefinedAt(sam.qname)) markDups(sam)
          sam
        }
        samIter
      }.map{ sam =>
        val bcMap = broadcastMap.value
        val idxedRname = bcMap.get(sam.rname).getOrElse(0) * 1000
        val dividedPos = (sam.pos / 100000000).toInt
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
      }.partitionBy(hashPtnr).map(_._2)
        .mapPartitions { Itr =>
          val res = Itr.toSeq.sortBy(_.pos).iterator
          res
        }

      rdd1.cache()

      val voutPath = s"$outPath/loop${"%03d".format(lcnt)}"

      rdd1.map(x => getString(x)).saveAsTextFile(voutPath)

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


