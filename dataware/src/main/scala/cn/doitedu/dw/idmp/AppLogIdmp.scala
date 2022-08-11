package cn.doitedu.dw.idmp

import cn.doitedu.commons.util.SparkUtil
import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

/**
 * 构建id映射字典
 */

object AppLogIdmp {

  def main(args: Array[String]): Unit = {

    // 1. 构建spark
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)

    // 2. 加载数据
    val ds: Dataset[String] = spark.read.textFile("data/yiee_logs/2020-01-11/app")
    // 3. 提取数据中每一行的标识字段
    val ids: RDD[Array[String]] = ds.rdd.map(line => {
      // 3.1 将每一行数据解析成json对象
      val jsonobj = JSON.parseObject(line)
      // 3.2 从json对象中提取user对象
      val userobj = jsonobj.getJSONObject("user")
      val uid = userobj.getString("uid")
      // 3.3 从user对象中取出phone对象
      val phoneobj = userobj.getJSONObject("phone")
      val imei = phoneobj.getString("imei")
      val mac = phoneobj.getString("mac")
      val imsi = phoneobj.getString("imsi")
      val androidId = phoneobj.getString("androidId")
      val deviceId = phoneobj.getString("deviceId")
      val uuid = phoneobj.getString("uuid")
      // 3.4 返回数组形式的结果
      Array(uid, imei, mac, imsi, androidId, deviceId, uuid).filter(StringUtils.isNotBlank(_))
    })

    // 4. 构造图计算中的vertex集合
    val vertices: RDD[(Long, String)] = ids.flatMap(arr => {
      for (biaoshi <- arr) yield (biaoshi.hashCode.toLong, biaoshi)
    })

    // 5. 构造图计算中的Edge集合
    val edges: RDD[Edge[String]] = ids.flatMap(arr => {
      // 同双层for循环，来对一个数组中所有的标识进行两两组合成边
      // [a,b,c,d] ==> a-b a-c a-d b-c b-d c-d
      for (i <- 0 to arr.length - 2; j <- i + 1 to arr.length - 1) yield Edge(arr(i).hashCode.toLong, arr(i).hashCode.toLong, "")
    }).map(edge => (edge, 1)) // 将边变成 (边,1)  来计算一个边出现的次数
      .reduceByKey(_ + _)
      .filter(tp => tp._2 > 2) // 过滤掉出现次数小于经验阈值的边
      .map(tp => tp._1)

    // 6. 用点集合+边集合构造图，并调用最大联通子图算法
    val graph: Graph[String, String] = Graph(vertices, edges)
    // VertexRDD[VertexId] ==> RDD[(点id-Long,组中的最小值)]
    val res_tuples: VertexRDD[VertexId] = graph.connectedComponents().vertices
    // 直接用图计算所产生的的结果中的组最小值，作为这一组的guid
    import spark.implicits._
    res_tuples.coalesce(1).toDF("biaoshi_hashcode","guid").write.parquet("data/idmp/2020-01-11")

    // 7. 关闭spark
    spark.close()
  }

}
