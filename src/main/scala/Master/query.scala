package Master

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.Queue
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

class kdTreeRangeQuery{
  val queue = new Queue[Node]
  val partitionNumbersArr = mutable.ArrayBuffer[Int]()
  def retrivalKdtree(node:Node,range:Double,queryData:Array[Int]):Array[Int]= {
    node match {
      case _: InternalNode =>
        val inode    = node.asInstanceOf[InternalNode]
        val splitDim = inode.split_dim

        val qp = queryData(splitDim)

        calRange(inode.left,inode, qp,splitDim,range)
        calRange(inode.right,inode, qp,splitDim,range)

        if(queue.isEmpty) {
          partitionNumbersArr.toArray
        }
        else {
          retrivalKdtree(queue.dequeue(), range, queryData)
        }


      case _: globalLeafNode =>
        val leafNode = node.asInstanceOf[globalLeafNode].partitionNumber
        partitionNumbersArr += leafNode

        if(queue.isEmpty) {
          partitionNumbersArr.toArray
        }
        else {
          retrivalKdtree(queue.dequeue(), range, queryData)
        }
    }
  }

  val leafNodeArr = ArrayBuffer[LeafNode]()
  def localRetrivalKdtree( //local tree 탐색
                           node      : Node,
                           range     : Double,
                           queryData : Array[Int]
                         ):Array[LeafNode]= {
    node match {
      case _: InternalNode =>
        val inode = node.asInstanceOf[InternalNode]
        val splitDim = inode.split_dim

        val qp = queryData(splitDim)

        if(inode.left  != null) calRange(inode.left, inode, qp, splitDim, range)
        if(inode.right != null) calRange(inode.right, inode, qp, splitDim, range)

        if (queue.nonEmpty)
          localRetrivalKdtree(queue.dequeue(), range, queryData)
        else
          leafNodeArr.toArray


      case _: LeafNode =>
        val leafNode = node.asInstanceOf[LeafNode]
        leafNodeArr += leafNode

        if (queue.nonEmpty)
          localRetrivalKdtree(queue.dequeue(), range, queryData)
        else
          leafNodeArr.toArray
    }
  }

  def calRange(
                node     : Node,
                inode    : InternalNode,
                qp       : Int,
                splitDim : Int,
                range    : Double
              )={
    var flag=0 //internalnode확인용
    var min=0
    var max=0

    node match { //internal과 leaf node가 min,max를 가지고 있는 방식이 다르므로 구분하여야 함. //지우면 에러남;
      case _: InternalNode =>
        min  = node.asInstanceOf[InternalNode].min
        max  = node.asInstanceOf[InternalNode].max

      case _: LeafNode =>
        min = node.asInstanceOf[LeafNode].mbr(splitDim).min
        max = node.asInstanceOf[LeafNode].mbr(splitDim).max

      case _:globalLeafNode =>
        min = node.asInstanceOf[globalLeafNode].mbr(splitDim).min
        max = node.asInstanceOf[globalLeafNode].mbr(splitDim).max
    }
    val dimDistance  = calDist(min, max, qp) // 노드의 차원의 거리
    val SumRange     = inode.asInstanceOf[InternalNode].range.sumRange //이전까지 더한 range
    val currentRange = SumRange + dimDistance

    if(currentRange <= range) {
      inode.range.dimensionDist += (splitDim -> dimDistance) //같은 차원에 대한 계산이면 추가를 하는게 아니라 업데이트를 해야한다.
      val InternalNodeRange = inode.asInstanceOf[InternalNode].range
      InternalNodeRange.sumRange = InternalNodeRange.dimensionDist.foldLeft(0.0)(_+_._2) //업데이트 된 결과를 다시 합산한다.
      queue.enqueue(node)
    }
  }

  def calDist(
               min : Int ,
               max : Int,
               qp  : Int
             ): Double = {
    if(qp >= min && qp <= max)
      0.0
    else {
      val calMax =  Math.sqrt(Math.pow(max-qp,2.0).abs)
      val calMin =  Math.sqrt(Math.pow(min-qp,2.0).abs)
      Math.pow(Math.min(
        Math.sqrt(calMax),Math.sqrt(calMin)
      ),2.0)
    }
  }
}

class KdTreeKnnQuery extends Serializable{
  def knnQuery(
                k           : Int,
                queryData   : Array[Int],
                kdTreeRoots : RDD[(Int, Master.InternalNode)]
              )={

    val queryDataLeafNode = treeSearch(ROOTINFO.root.get,queryData,kdTreeRoots) //일단 쿼리데이터가 위치할 global tree 의  leafnode 즉, 파티션을 찾아서
    val resultPartition   = queryDataLeafNode.map(node=>{//local tree를 탐색해야한다.
      locaTreeSearch(node, queryData, kdTreeRoots) //(pNumber,node)
    })
    resultPartition.count

    val leafNode = resultPartition//.take(1)(0) // k어차피 query data가 존재하는 partition은 한개이니까 그냥 꺼내자

//    println("INFO::쿼리 데이터가 존재하는 파티션은"+leafNode.partitionNumber+"번째 노드입니다.")

    val topK = selectTopKCandidate( //해당 파티션(쿼리데이터가 존재하는)에서 topK를 선택
      leafNode,
      queryData,
      k
    )
    topK
  }

  def treeSearch( // localKdTree까지 뽑아주세여
                  paramNode   : Node,
                  queryData   : Array[Int],
                  kdTreeRoots : RDD[(Int, Master.InternalNode)]
//                ):RDD[(Int, Master.InternalNode)]={
                ):RDD[Master.InternalNode]={
    paramNode match {
      case _: InternalNode =>
        val inode    = paramNode.asInstanceOf[InternalNode]
        val splitDim = inode.split_dim
        val splitVal = inode.split_val
        val realData = queryData(splitDim)


        if (realData <= splitVal)
          treeSearch(inode.left, queryData, kdTreeRoots)
        else
          treeSearch(inode.right, queryData, kdTreeRoots)

      case _: globalLeafNode => // localTree의 root를 탐색할 수 있도록
        val gNode    = paramNode.asInstanceOf[globalLeafNode]
        val realData = gNode.realData
        val pid      = gNode.partitionNumber

        val globalLeaf = kdTreeRoots
          .filter(rootInfo=>{
            val index = rootInfo._1
            val root = rootInfo._2
            index == pid
          }).map(x=>x._2)
        globalLeaf.count
        globalLeaf
    }
  }

//쿼리 포인트가 있는 lcoal tree leaf node를 return
  def locaTreeSearch( //treeSEarch 로컬로 나눈 이유가 하나로 하자니 local,global일떄의 return type이 두개다보니까 자꾸 에러뜬다
                      paramNode   : Node,
                      queryData   : Array[Int],
                      kdTreeRoots : RDD[(Int, Master.InternalNode)]
                    ):LeafNode={

    paramNode match {
      case _: InternalNode =>
        val inode    = paramNode.asInstanceOf[InternalNode]
        val splitDim = inode.split_dim
        val splitVal = inode.split_val
        val realData = queryData(splitDim)

        if (realData <= splitVal)
          locaTreeSearch(inode.left, queryData, kdTreeRoots)
        else
          locaTreeSearch(inode.right, queryData, kdTreeRoots)

      case _:LeafNode =>
        paramNode.asInstanceOf[LeafNode]
    }
  }
//
  def selectTopKCandidate(
                  leafNode  : RDD[LeafNode],
                  queryData : Array[Int],
                  k         : Int
                )={

    val topK = leafNode.map(qpnode=>{
      val nodeData = qpnode.realData.toArray
      val realDataDistance = nodeData
        .map(realData =>{
          calDistance(realData, queryData) //calDistance 함수 자체가 realdata,distance 이렇게 return해주므로
        })
      val topK = realDataDistance
        .sortBy(-_._2)
        .take(k)//range를 가장 먼걸 줘야하니까 역순으로
      topK
    })
    topK.count
    topK
  }

  def calDistance(
                   realData:(Array[Int], Array[String]),
                   queryData:Array[Int]
                 ):((Array[Int], Array[String]),Double) ={

    var sum      = 0.0
    val realdata = realData._1.map(_.toInt)

    for(dim <- 0 until 128) {
      val subValue = realdata(dim) - queryData(dim)
      sum = sum + Math.pow(subValue, 2.0)
    }
    (realData,Math.sqrt(sum))
  }
}
