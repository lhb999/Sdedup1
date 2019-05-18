
package V3


import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.SparkSession
import V3.Sdedup.getString
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

import scala.sys.process._
import org.apache.spark
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

object Test30 {

  case class SAM4(qname: String, flag: String, rname: String, pos: Long, others: String)


  def markDup(flag: Int) = {
    flag | 0x400
  }

  def markDups(sam: SAM4) = {
    val marked = markDup(sam.flag.toInt).toString
    val rebuild = SAM4(sam.qname, marked, sam.rname, sam.pos, sam.others)
    rebuild
  }

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

  def getString(sam: SAM4) = {
    val arr: Array[String] = Array(sam.qname, sam.flag, sam.rname, sam.pos.toString, sam.others)
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
    val log = LogManager.getRootLogger

    val tc = new TimeChecker
    tc.checkTime("start")

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
//    val ablsolFileInputPath = s"/lustre/hadoop/user/${sc.sparkUser}"
//        val ablsolFileInputPath = s"/lustre/hadoop/user/${sc.sparkUser}/$outPath"

    list.foreach{ fil =>
      val file = s"$filePath/$fil"
      val ttc = new TimeChecker
      ttc.checkTime(s"LOOP START\n")
      val far = if ((cur + stride) > len) len
      else {
        val tmp = cur + stride
        tmp
      }
      val l = list.slice(cur, far).map(x => s"$filePath$x")
      //      println(s"files : ${l.head} ~ ${l.last}")
      println(s"다음 파일을 읽습니다. : ${file}\t ${lcnt}/$len")
//      println(s"from : $cur to : ${far - 1} max : ${len - 1}")

      val sam = sc.textFile(file, 1)
      val sam1 = sam.filter(!_.startsWith("@")).map(x => parseToSAM(x)).map(x => (x.rname, 1)).reduceByKey(_+_)
      val res = sam1.map(x => s"${x._1}\t${x._2}").collect().mkString("\n")

      writeRnameInfo(ablsolFileOutputPath, file.split("/").last, res)
//      println(s"$file : $res")


      ttc.checkTime()
      val minsec = ttc.getElapsedTimeAsMinSeconds
      println(s"${lcnt} 번째 작업에 ${minsec} 걸렸습니다. ")

      lcnt += 1
      cur = far

    }
    tc.checkTime()
    val minsec = tc.getElapsedTimeAsMinSeconds
    println(s"총 소요시간은 ${minsec} 입니다.")
  }
  def writeRnameInfo(path:String, name:String, info:String) = {
    val Path = s"${path}/${name}.rname"
//    println(s"$path 경로를 만듭니다. ")
    new File(path).mkdirs()
    val partInfoFile = (new FileWriter(new File(Path), true)) //=> bufferedwriter로 교체
    partInfoFile.write(info)
    partInfoFile.flush()
    partInfoFile.close()
  }
}
