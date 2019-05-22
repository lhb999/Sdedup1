package V3


import java.io.{File, FileWriter}

import org.apache.log4j.LogManager
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.sys.process._

object Test50 {
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
    val hashPtnr = new HashPartitioner(partNum)
    val tc = new TimeChecker
    tc.checkTime("start")

    val accum = sc.longAccumulator("My Accumulator")
    println(s"SAM파일 입력 경로는 다음과 같습니다. ${filePath}")
    val samPathRegex = filePath + "*.sbl"

    val ablsolFileInputPath = s"/lustre/hadoop/user/${sc.sparkUser}/$filePath"
      .replaceAll("//","/")
    val ablsolFileOutputPath = s"/lustre/hadoop/user/${sc.sparkUser}/$outPath"
      .replaceAll("//","/")

//    val keyPathRegex = filePath + "/*.key"
//    val key1 = sc.textFile(keyPathRegex).flatMap(_.split("\n"))
//      .map { x =>
//        val spl = x.split("\t")
//        val qname = spl(0)
//        val dedupKey = spl(1).toLong
//        (dedupKey, (Seq(qname), 0))
//      }
//      .reduceByKey { (a, b) => //가상해시테이블-버킷 구현 필요
//        val v = a._2 + b._2 + 1
//        val res1 = a._1 ++ b._1
//        val res = (res1, v)
//        res
//      }.filter(x => x._2._2 > 0).flatMap(x => (x._2._1)).map(x => (x, Seq.empty[SAM4]))
//    key1.cache()

    tc.checkTime("startss")
    tc.printElapsedTime()
    println(s"절대경로로 치환했을 때 다음과 같습니다. ${ablsolFileInputPath}\n\n")

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

      //      println(s"files : ${l.head} ~ ${l.last}")
      println(s"files : ${currentSamfileList.mkString(",")}")
      println(s"from : $cur to : ${far-1} max : ${len-1}")

      val reducedByQname = sc.textFile(currentSamfileList.mkString(","), partNum)
        .filter(!_.startsWith("@"))
        .map(x => parseToSAM(x))
        .map(x => (x.qname, Seq(x)))
        .reduceByKey(_++_)
          .partitionBy(hashPtnr)

      println(s"Counts : ${reducedByQname.count()}")

      //      println(s" ===> Number of Marked Qname : ${accum.value} / ${totalRecords}")

      lcnt+=1
      cur = far
    }
    tc.checkTime()
    val minsec = tc.getElapsedTimeAsMinSeconds
    println(s" ===> TOTAL ELAPSED TIME : ${minsec}<===")
  }
}



