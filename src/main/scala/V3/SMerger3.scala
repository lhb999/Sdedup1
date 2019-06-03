package V3

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.sys.process._

object SMerger3 {
  var fileIdx = 0
  def dirSlash(dir:String) = {
    if(dir.endsWith("/")) dir.substring(0, dir.length-1)
    else dir
  }
  def setCmdWithDir1(dirname:String, rname:String, samtools:String, rmap:mutable.HashMap[String, Array[String]],
                     sambam:String="bam", thread:Int=4):mutable.Iterable[String] = {
    //println(s"serch partname : ${partname}")
    val rnameFiles = rmap.get(rname).get
    val fileLen = rnameFiles.length
    val arr = new ArrayBuffer[String]()
    var i = 0;
    val size = 5
    var loop = 1;
    var start = 0;
    var exitv = false
      while(start < fileLen) {
        val end =
          if(start + size > fileLen) {
            fileLen
          }
          else {
            start + size
          }
        val filex = rnameFiles.slice(start, end).mkString(" ")
        val cmd = s"$samtools merge -@ ${thread} -c -p -O ${sambam} $dirname/${"%06d".format(fileIdx)}-$rname.merged $filex"
        start =  end
        loop += 1
        fileIdx += 1
        arr.+=(cmd)
      }
    arr
  }
  def setCmdWithDir2(rnameWithPath:String, files:String, samtools:String, sambam:String="bam") = {
    val cmd = s"$samtools merge -@ 10 -c -p -O ${sambam} $rnameWithPath.${sambam} $files"
    cmd
  }

  def main(args: Array[String]): Unit = {
    if(args.length < 4) {
      println("usage sparkfrbs [dirPath] [samtoolsPath] [sam or bam] [samtools thread num]")
      System.exit(1)
    }
    val dirPath = dirSlash(args(0))
    val samtools = args(1)
    val sambam = args(2)
    val thread = args(3).toInt

    val tc = new TimeChecker

    println(s"Input Parh is ${dirPath}")

    val dirs =  s"ls $dirPath".!!.split("\\s+").filter(_.startsWith("loop"))

    val cmdArr = ArrayBuffer[String]()

    val rnameMap = new mutable.HashMap[String, Array[String]]()
    val proc2 = s"find $dirPath -name part.info".!!.split("\n")

    val splitArr = new mutable.ArrayBuffer[String]()

    proc2.foreach{info =>
      var cnt = 0
      val filename = info
      val curdir = filename.split("/").slice(0, filename.split("/").size-1).mkString("/")
      for (line <- Source.fromFile(filename).getLines) {
        if (cnt == 0) {
          println(s"file name is ${filename}")
          cnt += 1
        }
        if(!line.isEmpty) {
          val spl = line.split("\\s+")
          val partname = spl(0)
          val rnames = spl(1).split(",")
          if(rnames.length > 1) {
            println(s"INFO :: ${partname} has more than 2 rname : ${rnames.mkString(", ")}")
            splitArr.+=(filename)
          }

          rnames.foreach{ rname =>
            if(!rnameMap.isDefinedAt(rname)) {
//              println(s"new rname ${rname}")
              //println(s"map setted key : ${partname} value : ${rname}")
              rnameMap.put(rname, Array(curdir+"/"+partname))
            }
            else {
              val oldData = rnameMap.get(rname).get
              val newData = oldData ++ Array(curdir+"/"+partname)
              rnameMap.update(rname, newData)
            }
          }
        }
      }
    }

    var i = 0
    var toggle = true
//    println(s"rnames #: ${rnameMap.keys.size}: \n"+rnameMap.keys.mkString("\n"))
    rnameMap.keys.foreach{ rname =>
      val res = setCmdWithDir1(dirPath, rname, samtools, rnameMap, sambam, thread)
      res.foreach(x => cmdArr.+=(x))
    }

    val sparkConf = new SparkConf().setAppName("SMerger")
    val sc = new SparkContext(sparkConf)
    val cmdList = sc.makeRDD(cmdArr)

    cmdList.repartition(1024)



    tc.checkTime("start")
    println("Merge step 1 start ...")
    val act = cmdList.mapPartitionsWithIndex{(idx, itr) =>
      import scala.sys.process._

      itr.foreach{cmd =>
        cmd.!
      }
      itr
    }
    act.count()

    println("Merge step 2 start ...")
    val proc4 = (s"ls $dirPath" #| "grep merged").!!.split("\n")

    var map3 = mutable.Map[String, Array[String]]()
    proc4.foreach{fname =>
      val rname = fname.split("-")(1).dropRight(7)
      val path = s"$dirPath/$fname"

      if(map3.isDefinedAt(rname)) {
        val old = map3.get(rname).get
        val newVal = (old ++ Array(path))
        map3 = map3.updated(rname, newVal)
      }
      else {
        map3.put(rname, Array(path))
      }
    }

    val mergeSeq = map3.filter(_._2.length > 1).toSeq.sortBy(_._1)
    val moveSeq = map3.filter(_._2.length == 1).toSeq.map{x =>
      val isU = if(x._1.equals("*")) "zzzzz"
      else x._1
      (isU, x._2)
    }.sortBy(_._1)

    println(moveSeq.map(_._1).mkString("\n"))
    var cnt = 0

    val cmdList2 = mergeSeq.map{ x =>
      val rname = x._1
      val files = x._2.mkString(" ")
      val rnameWithPath = s"$dirPath/Output$cnt"
      cnt += 1
      val res = setCmdWithDir2(rnameWithPath, files, samtools, sambam)
      res
    }


    val cmdList3 = moveSeq.foreach{ x =>
      val rname = x._1
      val cntx = cnt
      val file = x._2.mkString(" ")
      val rnameWithPath = s"$dirPath/Output$cntx"
      cnt += 1
      val cmd = s"mv $file $rnameWithPath.${sambam}"
      //println(cmd)
      cmd.!
    }
    println("Merge step 3 start ...")

    val mergeList = sc.makeRDD(cmdList2).repartition(1024)
    val act2 = mergeList.mapPartitionsWithIndex{(idx, itr) =>
      import scala.sys.process._
      itr.foreach{cmd =>
        cmd.!
      }
      itr
    }
    act2.count()

    tc.checkTime("end")
    tc.printElapsedTime()
  }
}

