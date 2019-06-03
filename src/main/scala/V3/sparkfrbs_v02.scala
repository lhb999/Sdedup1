package V3


import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, TaskContext}
import scala.sys.process._
import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.io.Source


object sparkfrbs_v02 {
  def main(args : Array[String]): Unit = {
    if(args.length < 6) {
      println("usage : FreebayesPath SamtoolsPath ReferencefilePath WorkDir VcfOutputDirPath PartitionNumb RegionSize")
    }

    val sparkConf = new SparkConf().setAppName("sparkfrbs")
    val sc = new SparkContext(sparkConf)

    val FreebayesPath = args(0)
    val SamtoolsPath  = args(1)
    val Ref = args(2) //ReferencefilePath
    val Bamfile = args(3) //Sam or Bam file

    val OutputDirPath = args(4).endsWith("/") match {
      case true => args(4)
      case false => args(4)+"/"
    }

    println(s"vcf out dir is : $OutputDirPath")
    val ExecutorNumb = args(5).toInt
    val regionSize = args(6).toInt
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
    val stride = cmdList.length / 10
    val len = cmdList.length
    var cur = 0
    var lcnt = 0

    while(cur < len) {
      val ttc = new TimeChecker
      ttc.checkTime(s"LOOP START\n")
      val far = if((cur + stride) > len) len
      else {
        val tmp = cur + stride
        tmp
      }

      val sliced = cmdList.slice(cur, far)

      lcnt+=1
      cur = far
    }

    val res = sc.parallelize(cmdList, ExecutorNumb*10).mapPartitionsWithIndex{ (idx, iter) =>
      val pi = TaskContext.getPartitionId()
      iter.foreach { data =>
        val filename = Bamfile
        val fasta = data
        val vcfout = (s"$OutputDirPath${fasta.idx.formatted("%06d")}.${pi.formatted("%010d")}.${fasta.name}.${fasta.start.formatted("%013d")}-${fasta.end.formatted("%013d")}.vcf")
        println(filename)
        val cmd =
        ((s"$FreebayesPath -v $vcfout --pooled-discrete --pooled-continuous --min-alternate-fraction 0.1 --genotype-qualities " +
          s"--report-genotype-likelihood-max --allele-balance-priors-off -f $Ref $filename -r ${fasta.name}:${fasta.start}-${fasta.end}"))
        println(cmd)
        cmd.!
      }
      iter
    }

    res.count()


    helper.checkTime()
    helper.printElapsedTime()
  }
}



