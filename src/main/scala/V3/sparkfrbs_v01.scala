package V3


import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, TaskContext}

import scala.sys.process._
import java.io.{File, FileWriter}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.apache.log4j.{Level, Logger}

object sparkfrbs_v01 {
  def main(args : Array[String]): Unit = {
    if(args.length < 6) {
      println("usage : FreebayesPath SamtoolsPath ReferencefilePath WorkDir VcfOutputDirPath PartitionNumb RegionSize")
    }

    val sparkConf = new SparkConf().setAppName("sparkfrbs")
    val sc = new SparkContext(sparkConf)

    val FreebayesPath = args(0)
    val SamtoolsPath  = args(1)
    val Ref = args(2) //ReferencefilePath
    val parallelPath = args(3)
    val bamfilePath = args(4) //Sam or Bam file

    val OutputDirPath = args(5).endsWith("/") match {
      case true => args(5)
      case false => args(5)+"/"
    }

    println(s"vcf out dir is : $OutputDirPath")

    val ExecutorNumb = args(6).toInt
    val regionSize = args(7).toInt

    val FreebayesParallelPath = args(8)
    val ThreadNumb = args(9)
    //fasta!
    case class myfasta(name:String, idx:Int, start:Long, end:Long)

    //    var chromInfo = ArrayBuffer[(String, myfasta)]()
    val cmdList = ArrayBuffer[myfasta]()
    //    val cmdList2 = ArrayBuffer[(String, myfasta)]()

    val faiFile = Ref+".fai"

    val helper = new TimeChecker()
    helper.checkTime()

    var cnt = 0
    for (str <- Source.fromFile(faiFile).getLines) {
      var regionStart = 0
      val spl = str.split("\t")
      val chromName = spl(0)
      val chromLengh  = spl(1).toInt

      while(regionStart < chromLengh) {
        var start = regionStart
        var end = regionStart + regionSize
        if(end > chromLengh) end = chromLengh

        val fasta = myfasta(chromName, cnt, start, end)
        cmdList.+=(fasta)
        regionStart = end
        cnt = cnt + 1
      }
    }

    println(s"run freebayes ...")

    val sliced = cmdList.map(fasta => s"${fasta.name}:${fasta.start}-${fasta.end}")
    val len = sliced.length
    val stride = ThreadNumb.toInt

    val cmdBuffer = ArrayBuffer[String]()

    var cur = 0
    while(cur < len) {
      val far = if((cur + stride) > len) len
      else {
        val tmp = cur + stride
        tmp
      }
      val str = sliced.slice(cur, far).mkString("\n")
      cmdBuffer.+=(str)
      cur = far
    }

    val cmds = cmdBuffer.zipWithIndex.map{ x =>
      val data = x._1
      val idx = s"${OutputDirPath}region${"%02d".format(x._2)}"
      (data,idx)
    }

    sc.parallelize(cmds, ExecutorNumb*10).mapPartitionsWithIndex{ (idx, iter) =>
      val pi = TaskContext.getPartitionId()
      val act = iter.map { dat =>
        val data = dat._1
        val spl = data.split("\n")
        val vcfout = (s"$OutputDirPath${spl.head}-${spl.last.split('-').last}")
        val curPath = dat._2+".txt"
        val partInfoFile = new FileWriter(curPath, true)
        partInfoFile.append(data)
        partInfoFile.flush()
        partInfoFile.close()

        data
      }
      act
    }.count()



    val res = sc.parallelize(cmds, ExecutorNumb*10).mapPartitionsWithIndex{ (idx, iter) =>
      val pi = TaskContext.getPartitionId()
      val act = iter.map { dat =>
        val data = dat._1
        val spl = data.split("\n")
        val vcfout = dat._2+".vcf"

        //        val res = ((s"$FreebayesPath -v $vcfout --pooled-discrete --pooled-continuous --min-alternate-fraction 0.1 --genotype-qualities " +
        //          s"--report-genotype-likelihood-max --allele-balance-priors-off -f $Ref $filename -r ${fasta.name}:${fasta.start}-${fasta.end}")).!
        // freebayes-parallel <(fasta_generate_regions.py ref.fa.fai 100000) 36 -f ref.fa aln.bam >out.vcf

        val curPath = dat._2+".txt"

        val cmd = (s"$FreebayesParallelPath $FreebayesPath $parallelPath $curPath $ThreadNumb -f $Ref $bamfilePath")
        println(cmd+" >>> "+vcfout)
        val result = ((s"$cmd") #> new File(vcfout)).!

        cmd
      }
      act
    }


    println(res.collect().mkString("\n"))


    helper.checkTime()
    helper.printElapsedTime()
  }
}



