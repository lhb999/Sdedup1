package Master

import java.util
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

object main {
  def main(args : Array[String]) : Unit = {
    val set       = new initialization
    val generateG = new generateGlobalTree
    val generateL = new generateLocalTree

    val (sc,realData,hdi) = set.init(args) // 다변수를 받을때는 소문자로 시작해야합니다 ㅡㅡ

    val broadHdi        = sc.broadcast(hdi)
    val startTime       = System.currentTimeMillis()
    var broadKd         = generateG.generate(realData, broadHdi, sc)
    val rePartition     = generateG.globalTreePartition(realData, broadKd, broadHdi)
    println("INFO::로컬 트리 생성 시작....")
    val localTreeRoots  = generateL.generate(rePartition, broadHdi, sc) //데이터까지 두둑히 담긴 kd-tree return
    val endTime         = System.currentTimeMillis()

    println("INFO::로컬 트리까지 모두 생성하였습니다!! Total Time : "+(endTime - startTime) / 1000.0)

    val queryfun = new Query
    val range = 576
    val k     = 100
    queryfun.queryFunction(range, k, broadHdi, localTreeRoots, args(2))

  }

  class Query extends Serializable{
    def queryFunction(
                       range          : Int,
                       k              : Int,
                       broadHdi       : Broadcast[Master.HDIClass],
                       localTreeRoots : RDD[(Int, Master.InternalNode)],
                       arg            : String
                     ): Unit = {
      //쿼리
      val query = new queryProcess

      //쿼리데이터 읽어오기
      var ArrayQuery = new ArrayBuffer[Array[Int]]
      val bufferdSource = Source.fromFile(arg)
      for (line <- bufferdSource.getLines())
        ArrayQuery += line.split(" ").map(_.toInt)
      bufferdSource.close()

      //다양한 range정의
      //      var LimitRange = new Array[Int](10)
      //      LimitRange(0)  = 500
      //      LimitRange(1)  = 600
      //      LimitRange(2)  = 700
      //      LimitRange(3)  = 400
      //      LimitRange(4)  = 500
      //      LimitRange(5)  = 600
      //      LimitRange(6)  = 700
      //      LimitRange(7)  = 800
      //      LimitRange(8)  = 900
      //      LimitRange(9)  = 1000

      /** RANGE쿼리 **/
      for (rg <- 0 until 1) {
        for (j <- 0 until 1) {
          val queryData = ArrayQuery(j)
          println("쿼리데이터는" + queryData.mkString(" "))
          var startTime = System.currentTimeMillis()
          val rangeRes = query.rangeQuery(broadHdi, localTreeRoots, queryData, 288)
          rangeRes.count
          var endTime = System.currentTimeMillis()
          println("INFO::Rnage Query 완료!! Total Time : " + (endTime - startTime) / 1000.0)
          rangeRes.map(partition => {
            partition.map(leafNodes => {
              println("찾은개수는" + leafNodes.size + "개 입니다")
            })
          }).count
        }
      }

      /**KNN쿼리**/
      //      for(_<- 0 until 1){
      for(j <- 0 until 100) {
        println("\n\nINFO::K는 " + k + "입니다")
        val queryData = ArrayQuery(j)
        println("쿼리데이터는"+queryData.mkString(" "))
        val startTime = System.currentTimeMillis()
        val knnRes = query.knnQuery(k, queryData, broadHdi, localTreeRoots)

        val knnResTop = knnRes
          .flatMap{x =>
            val size = 1 //몇개까지 ?
            val v1 = x.map(x => (x.map(r => ((r._1._1.slice(0, size), r._1._2.slice(0, size)), r._2)))).map(x => x.head)
            v1
          }



        knnRes.count
        val endTime = System.currentTimeMillis()
        println("INFO::KNN Query 완료!! Total Time : " + (endTime - startTime) / 1000.0)

        //hb
        val collected = knnResTop.collect()
        collected.indices.foreach{ idx =>
          val record = collected(idx)
          val c1 = record._1._1.mkString(",")
          val c2 = record._1._2.mkString(",")

          println(s"인덱스 : $idx 거리 : ${record._2}")
          println(s"데이터 1 : ${c1}")
          println(s"데이터 2 : ${c2}")

        }

        /*
        knnRes.map(partition=>{
          partition.take(1).map(leaf=>{
            leaf.take(1).map(k=>{
            val range    = k._2
            println("거리: "+range)
          })
          })
        }).count
        */
      }


      //      }
    }
    def printArray[K](array:Array[K]) = println(array.mkString("Array(" , ", " , ")"))
  }

  class initialization{
    def init(args:Array[String]):(SparkContext,RDD[(Array[Int], Array[String])],HDIClass)={
      /**Spark 설정**/
      val conf: SparkConf = new SparkConf()
        .setAppName("ETRI_DistributedKdTree")
        .setMaster("spark://203.255.92.39:7077")

      conf
        .set("spark.executor.memory", "40g")
        .set("spark.driver.memory", "40g")
        .set("spark.driver.maxResultSize", "40g")
        .set("spark.network.timeout", "3600")

      val sc         = SparkContext.getOrCreate(conf)
      val rootLogger = Logger.getRootLogger()
      rootLogger.setLevel(Level.ERROR)

      /**변수 설정**/
      val hdi = new HDIClass

      hdi.file_name    = args(0)
      hdi.split_number = args(1).toInt

      val domain  = 256
      hdi.voc     = domain / hdi.data_domain

      hdi.globalSplitN    = 50
      hdi.globalVarianceN = 50
      hdi.localSplitN     = 50
      hdi.localVarianceN  = 50

      for (i <- 0 until hdi.dimension)
        hdi.mbr(i) = MBR()

      val RealData = sc
        .textFile("file:///" + hdi.file_name)
        .map(data =>(
          data.split(" ").take(hdi.featureSize).map(_.toInt),
          data.split(" ").takeRight(1))
        )

      (sc,RealData,hdi)
    }
  }
}


