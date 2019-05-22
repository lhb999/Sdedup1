package Master

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import org.apache.spark.rdd.RDD

object ROOTINFO extends  Serializable {
  var root = None: Option[Node]
  val partitionNumbersArr = ArrayBuffer[LeafNode]() //
  val partitionNumbersArrG = ArrayBuffer[globalLeafNode]() //
  val partitionDim = Map[Int,LeafNode]() //mbr 재계산할때 파티션 번호/leafnode
  var knnPartitionNumber = 0
  var rddArr = new Array[InternalNode](4)
}

class queryRange extends  Serializable {
  val dimensionDist = Map[Int,Double]()
  var sumRange      = 0.0
}

class HDIClass extends  Serializable {
  val leafNode_data_nubmer = 31
  var file_name = ""
  var split_number = 0

  val dimension = 128
  val ratio = 0.1
  val mbr = new Array[MBR](dimension)
  val data_domain = 32
  val featureSize = 128
  var voc = 0

  var globalSplitN    = 0
  var globalVarianceN = 0

  var localSplitN    = 0
  var localVarianceN = 0

  var partition_number = 0
  var kdtree_leafnode_number = 0
}

case class MBR(var max:Int=255,var min:Int=0)






