package V3


import java.io._
import java.nio.file.{Files, Paths}

import org.apache.log4j.LogManager
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.immutable.Stream.Empty
import scala.io.Source
import scala.sys.process._

object Test51 {
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

  def parseToSAM(str:String) : (String, Seq[SAM4]) = {
    val srcData = str
    val spl = str.split("\\s+")

    val dedupkey = spl(0)
    val qname =
      if(spl(1).contains('@')) spl(1).split('@').last
      else spl(1)
    val flag = spl(2)
    val rname = spl(3)
    val pos1 = spl(4)
    val pos = try {
      pos1.toLong
    } catch {
      case e:Exception => {
        e.printStackTrace()
        0L
      }
    }

    val others = spl.slice(5, spl.length)

    (dedupkey, Seq(SAM4(qname, flag, rname, pos, others)))
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
    sparkConf.set("spark.shuffle.blockTransferService", "nio")
    sparkConf.set("spark.executor.memoryOverhead", "16g")
    sparkConf.set("spark.driver.maxResultSize", "16g")

    val sc = new SparkContext(sparkConf)


    val filePath = slashRmver(args(0))
    val outPath = slashRmver(args(1))
    val partNum = args(2).toInt
    val stride = args(3).toInt
    val log = LogManager.getRootLogger

    val tc = new TimeChecker
    tc.checkTime("start")

    val accum = sc.longAccumulator("My Accumulator")

    val hashPtnr = new HashPartitioner(partNum)
    println("다음 파일에서 헤더를 읽습니다. "+ filePath + "/Output0.samsbl")
    val loadHeader = sc.textFile(filePath + "/Output0.samsbl", partNum).filter(_.startsWith("@"))
    ppp.setDic(loadHeader.collect())

    val broadcastMap = sc.broadcast(ppp.getDict)
    val broadcastHeader = sc.broadcast(ppp.getHead)
    println(s"partitions1 : ${loadHeader.getNumPartitions}")

    val keyPathRegex = filePath + "/*.key"
    println("다음 경로에서 중복제거 키 파일을 읽습니다. "+ keyPathRegex)
    val key1 = sc.textFile(keyPathRegex, partNum).flatMap(_.split("\n")).map { x =>
      val spl = x.split("\t")
      val dedupKey = spl(1)
      val qname =
        if(spl(0).contains('@')) spl(1).split('@').last.hashCode
        else spl(0).hashCode
      (dedupKey, (0, qname))
    }.reduceByKey{(a,b) =>
      val cnt = a._1 + b._1 + 1
      val qname = a._2
      (cnt, qname)
    }.filter(_._2._1 > 0)
      .map(x => (x._1, x._2._2))
      .repartition(partNum)
      .map(x => s"${x._1}\t${x._2}")
      .collect()
    //          .saveAsTextFile(filePath+"/filteredKey/")


    //    val mocmap = Map[String, Int]()
    //    val bKey = sc.broadcast(keymap)
    //    tc.checkTime("key loaded")
    //    tc.printElapsedTime()

    println(s"SAM파일 입력 경로는 다음과 같습니다. ${filePath}")
    val samPathRegex = filePath + "*.sbl"

    val ablsolFileInputPath = s"/lustre/hadoop/user/${sc.sparkUser}/$filePath"
      .replaceAll("//","/")
    val ablsolFileOutputPath = s"/lustre/hadoop/user/${sc.sparkUser}/$outPath"
      .replaceAll("//","/")

    println(s"절대경로로 치환했을 때 다음과 같습니다. ${ablsolFileInputPath}\n\n")



    val keypath = ablsolFileInputPath+"/filteredKey.txt"

    val pw = new PrintWriter(new File(keypath))

    key1.foreach(x => pw.println(x))
    pw.flush()
    pw.close

    val list = s"ls ${ablsolFileInputPath}".!!.split("\n")
    val samList = list.filter(_.endsWith("sbl"))
      .sortBy(x => x.slice(6, x.indexOf(".")).toInt)

    val rnameList = list.filter(_.endsWith("rname"))
      .sortBy(x => x.slice(6, x.indexOf(".")).toInt)

    val len = samList.length
    var lcnt = 0
    var cur = 0
    var totalRecords = 0L
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
      val currentSamfileList = samList.slice(cur, far).map(x => s"$filePath/$x")
      val currentRnameList = rnameList.slice(cur, far).map(x => s"$filePath/$x")

      println(s"files : ${currentSamfileList.head} ~ ${currentSamfileList.last}")
      println(s"from : $cur to : ${far-1} max : ${len-1}")

      val rnameCounts = sc.textFile(currentRnameList.mkString(","), partNum).flatMap(_.split("\n"))
        .map{ rnameWithCount =>
          val spl = rnameWithCount.split("\t")
          val k = spl(0)
          val v = spl(1).toInt
          (k,v)
        }.reduceByKey(_+_)

      val total_num_samrecords = rnameCounts.map(_._2).fold(0)(_+_)
      val expected_num_samrecords_partition = ((total_num_samrecords /partNum) * 1.1).toInt
      val collected1 = rnameCounts.collectAsMap()
      val collected2 = collected1.map(x => (x._1, get_num_keys_rname(x._2/(expected_num_samrecords_partition))))

      val rnameCountsMapBC =
        sc.broadcast(collected1
          .map(x => (x._1, get_num_keys_rname(x._2/(expected_num_samrecords_partition)))))

      val customPtnr = new customPtnr(collected2.toSeq, partNum)



      val reducedByQname = sc.textFile(currentSamfileList.mkString(","), partNum)
        .filter(!_.startsWith("@"))
        .map(x => (parseToSAM(x)))
        .mapPartitions{ record =>
          val key = Source.fromFile(keypath).getLines().map { x =>
            val spl = x.split("\t")
            val dedupKey = spl(0)
            val qname = spl(1).toInt
            (dedupKey, qname)
          }.toMap

          val bcMap = key
          val res = record.map{ x =>
            if(bcMap.isDefinedAt(x._1) && !bcMap.getOrElse(x._1, 0).equals(x._2.head.qname.hashCode)) {
              x._2.map{x =>
                accum.add(1)
                markDups(x)
              }
            }
            else x._2
          }
          res
        }
        .flatMap(x => x)
        .map{sam =>
          val rkey = rKey(sam.rname, sam.pos)
          (rkey,  Seq(sam))
        }.partitionBy(customPtnr)

      reducedByQname.cache()

      val cnts = reducedByQname.count()

      totalRecords += cnts



      val voutPath = s"$outPath/loop${"%03d".format(lcnt)}"
      reducedByQname.flatMap(x => x._2).mapPartitions{sam =>
        if(!sam.isEmpty){
          val head = Seq(broadcastHeader.value.mkString("\n")).iterator
          val strsam = sam.toSeq.sortBy(_.pos).map(x => getString(x))
          (head ++ strsam)
        }
        else sam
      }.saveAsTextFile(voutPath)

      println(s"${ablsolFileOutputPath} file out path ..")

      val nt = reducedByQname.flatMap(x => x._2).mapPartitionsWithIndex{ (idx, sam) =>
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

      reducedByQname.unpersist()
      ttc.checkTime()
      val minsec = ttc.getElapsedTimeAsMinSeconds
      println(s" ===> LOOP : ${lcnt} INTERVAL : ${minsec}<===")
      println("\n\n")
      lcnt+=1
      cur = far
    }
    println(s" ===> Number of Marked Qname : ${accum.value} / ${totalRecords}")
    tc.checkTime()
    val minsec = tc.getElapsedTimeAsMinSeconds
    println(s" ===> TOTAL ELAPSED TIME : ${minsec}<===")
  }
}



