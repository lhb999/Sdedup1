package V3


import java.io.{File, FileWriter}

import org.apache.log4j.LogManager
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.sys.process._

object Test42 {
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
    sparkConf.set("spark.driver.maxResultSize", "16g")

    val sc = new SparkContext(sparkConf)


    val filePath = slashRmver(args(0))
    val outPath = slashRmver(args(1))
    val partNum = args(2).toInt
    val stride = args(3).toInt


    val tc = new TimeChecker
    tc.checkTime("start")

    val accum = sc.longAccumulator("My Accumulator")
    //
    //    println("다음 파일에서 헤더를 읽습니다. " + filePath + "/Output0.samsbl")
    //    val loadHeader = sc.textFile(filePath + "/Output0.samsbl", partNum).filter(_.startsWith("@"))
    //
    //    ppp.setDic(loadHeader.collect())
    //    println("헤더 로드 완료.")
    //    val broadcastMap = sc.broadcast(ppp.getDict)
    //    val broadcastHeader = sc.broadcast(ppp.getHead)
    //    //    println(s"partitions1 : ${loadHeader.getNumPartitions}")
    val hashPtnr = new HashPartitioner(partNum)

    val keyPathRegex = filePath + "/*.key"
    println("다음 경로에서 중복제거 키 파일을 읽습니다. " + keyPathRegex)

    //    val key1 = sc.textFile(keyPathRegex, partNum).flatMap(_.split("\n"))
    val key1 = sc.textFile(keyPathRegex).flatMap(_.split("\n"))
    key1.cache()
    println(key1.count()) //금방끝남
    val key2 = key1.map { x =>
      val spl = x.split("\t")
      val dedupKey = spl(1).toLong
      //      (dedupKey, (qname, 0))
      dedupKey
    }
//      .reduceByKey(_+_+1)
//      .filter(_._2 > 1)
      .saveAsTextFile("test/world-8")
    //    key2.cache()
    //    println(key2.count()) //금방끝남
    //    //    val key3 = key2.partitionBy(hashPtnr)
    //    val key3 = key2.repartition(partNum)
    //    key3.cache()
    //    //    val tt = key3.mapPartitionsWithIndex{ (idx, iter) =>
    //    //      println(s"part${idx} : ${iter.length}")
    //    //      iter
    //    //    }
    //    //    tt.count()
    //    println(key3.count())
    //    //금방끝남
    //    val key4 = key3.reduceByKey { (a, b) =>
    //      val v = a._2 + b._2 + 1
    //      val res1 = a._1 + "\n" + b._1
    //      val res = (res1, v)
    //      res
    //    }
    //
    //    //      .repartition(partNum)
    //    //      .saveAsTextFile("test/hello") //오래걸림
    //    key4.cache()
    //    println(key4.first()) //금방끝남
    //    key4.filter(_._2._2 > 0).repartition(partNum).saveAsTextFile("/test/catt")
    //
    //    val tt = key4.mapPartitionsWithIndex { (idx, iter) =>
    //      println(s"part${idx} : ${iter.length}")
    //      iter
    //    }
    //    tt.count()


    //    println(key4.count()) //오래걸림
    //
    //    key4.cache()
    //    val key5 = key4.filter(x => x._2._2 > 0)
    //    key5.cache()
    //    println(key5.count())//오래걸림
    //      key5.flatMap(x => (x._2._1))


    //    println(s"key counts : ${key1.count()}")
    //    val ts = key1.collect()

    tc.checkTime()
    tc.printElapsedTime()
    //      .partitionBy(hashPtnr) //qname, Int


    //    key1.cache()
    //
    tc.checkTime("key loaded")
    tc.printElapsedTime()
    //
    //    println(s"SAM파일 입력 경로는 다음과 같습니다. ${filePath}")
    //    val samPathRegex = filePath + "*.sbl"
    //
    //    val ablsolFileInputPath = s"/lustre/hadoop/user/${sc.sparkUser}/$filePath"
    //      .replaceAll("//","/")
    //    val ablsolFileOutputPath = s"/lustre/hadoop/user/${sc.sparkUser}/$outPath"
    //      .replaceAll("//","/")
    //
    //    println(s"절대경로로 치환했을 때 다음과 같습니다. ${ablsolFileInputPath}\n\n")
    //
    //    val list = s"ls ${ablsolFileInputPath}".!!.split("\n")
    //    val samList = list.filter(_.endsWith("sbl"))
    //      .sortBy(x => x.slice(6, x.indexOf(".")).toInt)
    //
    //    val rnameList = list.filter(_.endsWith("rname"))
    //      .sortBy(x => x.slice(6, x.indexOf(".")).toInt)
    //
    //    val len = samList.length
    //    var lcnt = 0
    //    var cur = 0
    //
    //    var totalRecords = 0L
    //
    //
    //    def get_num_keys_rname(value:Int) = {
    //      val res =
    //        if(value == 0 ) 1
    //        else value
    //
    //      res
    //    }
    //    while(cur < len) {
    //      val ttc = new TimeChecker
    //      ttc.checkTime(s"LOOP START\n")
    //      val far = if((cur + stride) > len) len
    //      else {
    //        val tmp = cur + stride
    //        tmp
    //      }
    //      val currentSamfileList = samList.slice(cur, far).map(x => s"$filePath/$x")
    //      val currentRnameList = rnameList.slice(cur, far).map(x => s"$filePath/$x")
    //
    //      //      println(s"files : ${l.head} ~ ${l.last}")
    //      println(s"files : ${currentSamfileList.mkString(",")}")
    //      println(s"from : $cur to : ${far-1} max : ${len-1}")
    //
    //      val rnameCounts = sc.textFile(currentRnameList.mkString(","), partNum)
    //        .map{ rnameWithCount =>
    //          val spl = rnameWithCount.split("\t")
    //          val k = spl(0)
    //          val v = spl(1).toInt
    //          (k,v)
    //        }.reduceByKey(_+_)
    //
    //      val total_num_samrecords = rnameCounts.map(_._2).fold(0)(_+_)
    //      val expected_num_samrecords_partition = ((total_num_samrecords /partNum) * 1.1).toInt
    //      val collected1 = rnameCounts.collectAsMap()
    //      val collected2 = collected1.map(x => (x._1, get_num_keys_rname(x._2/(expected_num_samrecords_partition))))
    //
    //      val rnameCountsMapBC =
    //        sc.broadcast(collected1
    //          .map(x => (x._1, get_num_keys_rname(x._2/(expected_num_samrecords_partition)))))
    //
    //      //      println("expected_num_samrecords_partition : "+expected_num_samrecords_partition)
    //
    //      //DBG
    //      val tmp = rnameCountsMapBC.value.map(x => s"${x._1}\t${x._2}")
    //      val customPtnr = new customPtnr(collected2.toSeq, partNum)
    //
    //
    //      val reducedByQname = sc.textFile(currentSamfileList.mkString(","), partNum)
    //        .filter(!_.startsWith("@"))
    //        .map(x => parseToSAM(x))
    //        .map(x => (x.qname, Seq(x)))
    //        .partitionBy(hashPtnr)
    //        //        .union(key1)
    //        //        .reduceByKey{ (a,b) =>
    //        //          accum.add(1)
    //        //          val c = (a++b).map(x => markDups(x))
    //        //          c
    //        //        }
    //        .mapPartitions{ record =>
    //        record.map(_._2)
    //      }.flatMap(x => x)
    //        .map{sam =>
    //          val rkey = rKey(sam.rname, sam.pos)
    //          (rkey,  Seq(sam))
    //        }.partitionBy(customPtnr)
    //
    //      reducedByQname.cache()
    //      val cnts = reducedByQname.count()
    //
    //      totalRecords += cnts
    //      println(s" ===> Number of Marked Qname : ${accum.value} / ${totalRecords}")
    //
    //
    //
    //      val voutPath = s"$outPath/loop${"%03d".format(lcnt)}"
    //      reducedByQname.flatMap(x => x._2).mapPartitions{sam =>
    //        if(!sam.isEmpty){
    //          val head = Seq(broadcastHeader.value.mkString("\n")).iterator
    //          val strsam = sam.toSeq.sortBy(_.pos).map(x => getString(x))
    //          (head ++ strsam)
    //        }
    //        else sam
    //      }.saveAsTextFile(voutPath)
    //
    //      println(s"${ablsolFileOutputPath} file out path ..")
    //
    //      val nt = reducedByQname.flatMap(x => x._2).mapPartitionsWithIndex{ (idx, sam) =>
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
    //      }.count()
    //
    //      reducedByQname.unpersist()
    //      ttc.checkTime()
    //      val minsec = ttc.getElapsedTimeAsMinSeconds
    //
    //      //      println(s" ===> Number of Marked Qname : ${accum.value} / ${totalRecords}")
    //      println(s" ===> LOOP : ${lcnt} INTERVAL : ${minsec}<===")
    //      println("\n\n")
    //      lcnt+=1
    //      cur = far
    //    }
    //    tc.checkTime()
    //    val minsec = tc.getElapsedTimeAsMinSeconds
    //    println(s" ===> TOTAL ELAPSED TIME : ${minsec}<===")
    //  }
  }
}