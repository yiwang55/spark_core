import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TransformationDemo4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TransformationDemo4").setMaster("local")
    val sc = new SparkContext(conf);

    //mapPartitions遍历出集合(RDD)中每一个元素,并对元素可以进一步操作
    val rdd1 = sc.parallelize(List(1,2,3,4,5,6),3)
    //mapPartitions是对每个分区中数据进行迭代
    val rdd2: RDD[Int] = rdd1.mapPartitions(_.map(_*10))
    println(rdd2.collect().toList)

    //mapPartitionswithIndex 是对RDD中每个分区的遍历操作
    val Iter = (index:Int,iter:Iterator[(Int)])=>{
      iter.map(x => "[partID:"+index+",value:"+x+"]")
    }

    val rdd3: RDD[String] = rdd1.mapPartitionsWithIndex(Iter)
    println(rdd3.collect().toList)

    //sortBy  是一个排序算子
    // 第一个参数 对RDD中数据的一种处理方式
    //第二个参数是决定排序的顺序 默认是为true 即升序  false 是降序
    val rdd4 = sc.parallelize(List(5,3,2,1,4,6,9,7,8))
    val rdd4_1 =  rdd4.sortBy(x => x,false)
    println(rdd4_1.collect().toList)

    //sortBykey 和 sortBy 类似 但是没有sortBy灵活
    //是根据key 进行排序的 并且这个key需要实现ordered接口
    //参数 是true 或false 默认是true即升序  false 降序
    val rdd5 = sc.parallelize(Array((3,"aa"),(6,"bb"),(1,"cc")))
    val rdd5_1: RDD[(Int, String)] = rdd5.sortByKey()
    println(rdd5_1.collect().toList)

  }
}
