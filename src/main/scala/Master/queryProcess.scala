package Master

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class queryProcess extends  Serializable{
  /**range 쿼리 부가함수**/
  def rangeQueryProcess(
                         range       : Double,
                         queryData   : Array[Int],
                         kdTreeRoots : RDD[(Int, Master.InternalNode)]
                       ) ={
    println("===Range 쿼리===");println("INFO::Range "+range+"로 질의를 진행합니다.")

    val rangeQueryClass = new kdTreeRangeQuery()
    val resultRddRootPartitionNumbers = rangeQueryClass.retrivalKdtree(ROOTINFO.root.get, range, queryData) // 일단 기본tree(분산tree말고)를 탐색후 파티션을 리턴받음
    //여기까지가 원래 slave랑 붙어있을때 했던 곳이라 이 파티션결과를 가지고 btree를 생성했었지만 이 후에 btree를 생성하는게 아니라 이 파티션안에는 kd-tree가 존재하므로 kd-tree 탐색을 한번 더 진행하는것

//    for(pid <- resultRddRootPartitionNumbers)
////      println("후보 파티션의 번호는"+pid)

    val leafNodes = kdTreeRoots.filter(rootInfo => { //local kdtree root를 돌면서 globla tree에서 준 partition number와 일치하는 root만 get
        val index = rootInfo._1
        val root  = rootInfo._2
        resultRddRootPartitionNumbers.contains(index)
      }) //local tree의 root

    val result = leafNodes.map(rootInfo=> { //global tree의 leafnode ,즉 local tree의 root마다 partition후보를 Array()로 리턴해 받음
      val rddRangeQueryClass  = new kdTreeRangeQuery()
      val root = rootInfo._2
      val res  = rddRangeQueryClass.localRetrivalKdtree(root, range, queryData) // 각 파티션의 root를 순서대로 돌수 있도록~
      res
    })
    result.count

    result
  }

  def calDataRange(
                    LeafNode  : Array[LeafNode],
                    queryData : Array[Int],
                    range     : Double,
                    broadHdi  : Broadcast[Master.HDIClass]
                  )  ={ //후보 ㅍㅏ티션내의 데이터들이 range안에 포함되는 데이터인지 구하는 함수

    val realDataArr = LeafNode.map(leafNode=>{
      val realDataArrayBuffer = new ArrayBuffer[((Array[Int], Array[String]),Double)] // range안에 존재하는 realdata를 담을 그릇
      val nodeData  = leafNode.asInstanceOf[LeafNode].realData

      for(realData <- nodeData){
        var sum = 0.0
        for(dim <- 0 until broadHdi.value.dimension) {
          val realFeaterData = realData._1.map(_.toInt)
          val subValue       = realFeaterData(dim) - queryData(dim)

          sum = sum + Math.pow(subValue, 2.0)
        }
        val distance = Math.sqrt(sum)
        if(distance < range) {
          val realDataDistance = (realData, distance)
          realDataArrayBuffer += realDataDistance//range도 같이 뱉어내서 knn쿼리ㅏㄹ때 정렬할수있도록
        }
      }
      if(realDataArrayBuffer.nonEmpty)
        realDataArrayBuffer.toArray
      else
        null
    })
    
    realDataArr.filter(leafNode => leafNode != null)
  }

  /**쿼리 함수 시작**/
  /**range 쿼리 시작점 **위에다가 함수 안올리면 에러나서 ㅠㅠ 이게모임**/
  def rangeQuery(
                  broadHdi    : Broadcast[Master.HDIClass],
                  kdTreeRoots : RDD[(Int, Master.InternalNode)],
                  queryData   : Array[Int],
                  range       : Double
                ) ={
    println("INFO::Range 쿼리 테스트")
    //range내의 후보 파티션 선택
    val resultLeafNode = rangeQueryProcess(range,queryData,kdTreeRoots)

    println("INFO:후보 파티션의 데이터와 queryData를 비교하여 range 안의 데이터를 찾습니다...")
    val res = resultLeafNode.map(leafNode=>
        calDataRange(leafNode,queryData,range,broadHdi)
      )
    res.count
    res//그럼 각 후보파티션 별로 data를 찾았을거임
  }

  /**knnQuery 시작**/
  def knnQuery(
                k            : Int,
                queryData    : Array[Int],
                broadHdi    : Broadcast[Master.HDIClass],
                kdTreeRoots  : RDD[(Int, Master.InternalNode)]
              )= {
    println("INFO::Knn 쿼리 테스트")
    val knnQueryClass   = new KdTreeKnnQuery()
    var qpPartitionTopK   = knnQueryClass.knnQuery(k, queryData, kdTreeRoots) //querydata가 있는 partition을 찾아용
    var range = qpPartitionTopK.take(1)(0)(0)._2// 가장 거리가 먼 (0)이 가장멀고 (k)가 가장 가까움 take하면 array로 리턴하니 array(array(topK)) 이렇게 댄다 정말 더러운 코드군..
    // partitionTopK 형식은 RDD[(Int, Array[((Array[Int], Array[String]), Double)])] 앞에 Int가 pNumber임
    println("INFO::첫 번째 파티션을 검사하여 Range "+range+"를 정의하였습니다.")

    println("INFO::재정의 된 range로 후보 파티션을 찾습니다.")
    val rangeQueryResult = rangeQuery(broadHdi, kdTreeRoots, queryData, range) //range를 이용해서 rangeQuery를 날려 다른 후보 파티션을 골라냄
    //rangeQueryResult //range 내의 데이터들을 받았을것이다.

    val sortRange = rangeQueryResult.map{partition =>
      val res = partition
        .map(leafnode => leafnode
          .sortBy( topK => topK._2))
        .sortBy(leafnode => leafnode.head._2) //정렬
      res
    }
    sortRange.count

    sortRange
  }
}
