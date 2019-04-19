package V3


import java.io.{File, FileWriter}

import V3.Sdedup.getString
import V3.{SAM4, String2SAM}
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: APP <SAM OUT PATH> <OUTPUT PATH>")
      System.exit(1)
    }

    val hashPtnr = new spark.HashPartitioner(2048)
    val sparkConf = new SparkConf().setAppName("UNKNOWN-DEDUP")
    val sc = new SparkContext(sparkConf)

    val filePath = args(0)
    val outPath = args(1)

    val partNum = 0

    //    val key = sc.textFile(filePath, partNum).flatMap(_.split("\n")).map(x => ())
    val keyPathRegex = filePath + "*.key"
    val key = sc.textFile(keyPathRegex, partNum).flatMap(_.split("\n"))
      .map { x =>
        val spl = x.split("\t")
        val qname = spl(0)
        val key = spl(1)
        (key, (qname, 0))
      }.reduceByKey { (a, b) =>
      val v = a._2 + b._2 + 1
      (a._1, v)
    }.filter(_._2._2 > 2).map(x => (x._1, Array[SAM4]())).cache()

    println("1")
    val samPathRegex = filePath + "*.sam"

    val loadHeader = sc.textFile(filePath + "Output0.sam").filter(_.startsWith("@"))
    ppp.setDic(loadHeader.collect())

    val broadcastMap = sc.broadcast(ppp.getDict)
    val broadcastHeader = sc.broadcast(ppp.getHead)

    val sam = sc.textFile(samPathRegex, partNum).filter(!_.startsWith("@")).flatMap(_.split("\n"))
      .map(x => String2SAM.parseToSAM(x)).map(x => (x.qname, Array[SAM4](x)))

    val rdd = sam.union(key).reduceByKey { (a, b) =>
      val c = a ++ b
      val res = c.map(x => String2SAM.markDups(x))
      res
    }.map(_._2)

    println("2")

    val pairedSam = rdd.map { x =>
      val paired = x.map { sam =>
        val bcMap = broadcastMap.value
        val idxedRname = bcMap.get(sam.rname).getOrElse(0) * 1000
        val dividedPos = (sam.pos / 100000000).toInt
        val key = idxedRname + dividedPos
        val res = (key, sam)
        res
      }
      paired
    }.flatMap(x => x).partitionBy(hashPtnr)
    println("3")

    val formattedStreamCnt = "%05d".format(0)
//    val makePartitionInfoFile = pairedSam.map(x => x._2.rname).mapPartitionsWithIndex { (idx, iter) =>
//      if (!iter.isEmpty) {
//        val rnames = iter.toSeq.distinct.mkString(",").replace("*", "unmapped")
//        val partitionNo = "%05d".format(idx)
//        val streamDir = new File(s"${outPath}/stream-${formattedStreamCnt}")
//        streamDir.mkdirs()
//        val partInfoFile = new FileWriter(s"${outPath}/stream-${formattedStreamCnt}/part.info", true)
//        partInfoFile.append(s"part-${partitionNo}\t${rnames}\n")
//        partInfoFile.close()
//      }
//      iter
//    }
//    makePartitionInfoFile.count() //do action
//    println("4")

    val sortedSam = pairedSam.map(_._2)
      .mapPartitions { samIter =>
        val head = Seq(broadcastHeader.value.mkString("\n")).iterator
        val sorted = samIter.toSeq.sortBy(_.pos).map(x => getString(x)).iterator
        val res = if (sorted.isEmpty) sorted
        else head ++ sorted
        res
      }
    println("5")
    sortedSam.saveAsTextFile(s"${outPath}/stream-${formattedStreamCnt}")
  }
}

