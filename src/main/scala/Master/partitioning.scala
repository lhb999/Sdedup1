// 사용방법 var y = orginaldata.partitionBy(new kdtreePartitione(spi.split_number))//

package Master
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import scala.annotation.tailrec
import org.apache.spark.broadcast.Broadcast

object searchNode extends Serializable {
  var splitDim = 0
  var realData =0

  //global tree 구축
  @tailrec
  def searchLeaf(node:Option[Node],data:(Array[Int])):Int= { //data는 문서상의 realdata를 가지고 있다.
    node.get match {
      case _: InternalNode =>
        val inode    = node.get.asInstanceOf[InternalNode]
        val splitVal = inode.split_val
        splitDim = inode.split_dim
        realData = data(splitDim).toInt

        if (realData <= splitVal)
          searchLeaf (Some(inode.left), data)
        else
          searchLeaf (Some(inode.right), data)

      case _: globalLeafNode =>
        node.get.asInstanceOf[globalLeafNode].partitionNumber
    }
  }

  //local tree 구축할때 데이터 바로 넣을 수 있게 하는 변형 코드
  def localTreeSearchLeaf(
                           node:Node,
                           paramData:(Array[Int], Array[String])
                         ):Unit = { //data는 문서상의 realdata를 가지고 있다.
    node match {
      case _: InternalNode=>
        val inode    = node.asInstanceOf[InternalNode]
        val splitVal = inode.split_val
        splitDim = inode.split_dim
        realData = paramData._1(splitDim).toInt

        if (realData <= splitVal)
          localTreeSearchLeaf(inode.left, paramData)
        else
          localTreeSearchLeaf(inode.right, paramData)

      case _: LeafNode =>
        node.asInstanceOf[LeafNode].realData += paramData //바로 leafNode에 값을 가질 수 있도록 한다.
    }
  }
}

class kdtreePartitioner(partitions: Int, rootNode:Broadcast[Master.ROOTINFO.type]) extends Partitioner {
  def numPartitions = partitions
  val node          = rootNode.value.root
  def getPartition(key:Any): Int = {
    val data = key.asInstanceOf[Array[Int]]
    searchNode.searchLeaf(node,data) % numPartitions
  }
}


