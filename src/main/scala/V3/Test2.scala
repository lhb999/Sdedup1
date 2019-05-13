package V3


import java.io.{File, FileWriter}
import org.apache.spark.sql.SparkSession
import V3.Sdedup.getString
import V3.{SAM4, String2SAM}
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

object Test2 {
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
    val hashPtnr = new spark.HashPartitioner(partNum)

//    val Spark = SparkSession
//      .builder()
//      .appName("Spark SQL basic example")
//      .config("spark.some.config.option", "some-value")
//      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
//    import Spark.implicits._

    //    val key = sc.textFile(filePath, partNum).flatMap(_.split("\n")).map(x => ())

    val tc = new TimeChecker
    tc.checkTime("start")

    val loadHeader = sc.textFile(filePath + "Output0.sam").filter(_.startsWith("@"))
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
      res
    }.filter(_._2._2 > 2).map(x => (x._1, (x._2._2, Seq.empty[SAM4]))).cache()

    println(s"READ PATH IS ${filePath}")

    val samPathRegex = filePath + "*.sbl"
    val sam = sc.textFile(samPathRegex, partNum).filter(!_.startsWith("@")).flatMap(_.split("\n"))
      .map(x => String2SAM.parseToSAM(x)).map(x => (x.qname, (0,Seq(x))))


    val rdd = sam.union(key).reduceByKey{ (a,b) =>
      val v = a._1 + b._1
      val res = if(v > 10)(a._2++b._2).map(x => String2SAM.markDups(x))
      else (a._2++b._2)
      //        .map(x => String2SAM.markDups(x))
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
//      .sortByKey()
//      .sortByKey(true,partNum)
//      .repartition(partNum)
      .partitionBy(hashPtnr)
      .map(x => x._2)
      .mapPartitions { samIter =>
      val head = Seq(broadcastHeader.value.mkString("\n")).iterator
      val sorted = samIter.toSeq.sortBy(_.pos).map(x => getString(x)).iterator
      val res = if (sorted.isEmpty) sorted
      else head ++ sorted
      res
    }
//



    rdd
//      .flatMap(x => x)
//      .map(x => getString(x))
      .saveAsTextFile(outPath)

    tc.checkTime("END")

//    val deduped = key.map(x => x._2._1).fold(0)(_+_) / 10
//    val recordNumb = key.count()
//    println(s" $deduped / $recordNumb")

    tc.printElapsedTime()

    //
    //

  }
}

