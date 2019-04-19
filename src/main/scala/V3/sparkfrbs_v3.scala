package V3


import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, TaskContext}
import scala.sys.process._
import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.apache.log4j.{Level, Logger}

object sparkfrbs_v3 {
  def main(args : Array[String]): Unit = {
    if(args.length < 6) {
      println("usage : FreebayesPath SamtoolsPath ReferencefilePath WorkDir VcfOutputDirPath PartitionNumb RegionSize")
    }

    val sparkConf = new SparkConf().setAppName("sparkfrbs")
    val sc = new SparkContext(sparkConf)

    val rootLogger      = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val FreebayesPath = args(0)
    val SamtoolsPath  = args(1)
    val Ref = args(2) //ReferencefilePath
    val WorkDir = args(3) //Sam or Bam file

    val OutputDirPath = args(4).endsWith("/") match {
      case true => args(4)
      case false => args(4)+"/"
    }

    println(s"vcf out dir is : $OutputDirPath")
    val ExecutorNumb = args(5).toInt
    val regionSize = args(6).toInt

    val FreebayesParallelPath = args(7)
    val ThreadNumb = args(8)
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
    val fileList = (s"find $WorkDir -name *.bam -type f -ls").!!.split("\n")
    val cmdList2 = fileList.map{ file =>
      val fullpath = file.split("\\s+")(10)
      val rname = fullpath.substring(0, fullpath.length-4).split("/").last
      val cmd = cmdList.filter(_.name==rname).map(x => (fullpath, x))
      cmd
    }.flatMap(x => x)

    println(s"run freebayes ...")

    val tmp = cmdList2.groupBy(x => x._1).map{ x =>
      val key = x._1
      val dat = x._2.map(x => x._2)
      (key, dat)
    }.toSeq

    val res = sc.parallelize(tmp, ExecutorNumb*10).mapPartitionsWithIndex{ (idx, iter) =>
      val pi = TaskContext.getPartitionId()
      val act = iter.foreach { data =>
        val filename = data._1
        val fastaz = data._2
        val fasta = data._2(0)
        val fastaStr = data._2.map{ fasta =>
          val res = s"${fasta.name}:${fasta.start}-${fasta.end}"
        }.mkString("\n")
        val vcfout = (s"$OutputDirPath${fasta.idx.formatted("%06d")}.${pi.formatted("%010d")}.${fasta.name}.${fasta.start.formatted("%013d")}-${fasta.end.formatted("%013d")}.vcf")
        if(!(new File(s"$filename.bai")).exists()) s"${SamtoolsPath} index -@ $ThreadNumb $filename".!!
        val outputdir = (new File(s"$OutputDirPath"))
        if(!outputdir.exists())  outputdir.mkdirs()

//        val res = ((s"$FreebayesPath -v $vcfout --pooled-discrete --pooled-continuous --min-alternate-fraction 0.1 --genotype-qualities " +
//          s"--report-genotype-likelihood-max --allele-balance-priors-off -f $Ref $filename -r ${fasta.name}:${fasta.start}-${fasta.end}")).!
        // freebayes-parallel <(fasta_generate_regions.py ref.fa.fai 100000) 36 -f ref.fa aln.bam >out.vcf
        val result = ((s"$FreebayesParallelPath $FreebayesPath $fastaStr \\$FreebayesPath $ThreadNumb -f $Ref $filename --pooled-discrete --pooled-continuous --min-alternate-fraction 0.1 --genotype-qualities " +
          s"--report-genotype-likelihood-max --allele-balance-priors-off") #> new File(vcfout)).!
        result
      }
      iter
    }

    res.count()


    helper.checkTime()
    helper.printElapsedTime()
  }
}



