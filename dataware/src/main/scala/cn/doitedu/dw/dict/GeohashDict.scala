package cn.doitedu.dw.dict

import ch.hsr.geohash.GeoHash
import cn.doitedu.commons.util.SparkUtil

import java.util.Properties

/**
 * 构建地理位置字典
 */

object GeohashDict {

  def main(args: Array[String]): Unit = {

    // TODO 1. 构建spark
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    // TODO 2. 读取mysql数据
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "Guo_2001")
    val df = spark.read.jdbc("jdbc:mysql://hadoop01:3306/dicts", "tmp", props)

    // TODO 3. 将GPS坐标通过geohash算法转为geohash编码
    val res = df.map(row => {
      // 3.1 取出这一行的经度、纬度及对应的省、市、区
      val lng = row.getAs[Double]("BD09_LNG")
      val lat = row.getAs[Double]("BD09_LAT")
      val district = row.getAs[String]("district")
      val city = row.getAs[String]("city")
      val province = row.getAs[String]("province")
      // 3.2  调用geohash算法，得出geohash编码
      val geoCode = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 5)
      // 3.3 组装返回结果 (geohash编码,省,市,区)
      (geoCode, province, city, district)
    }).toDF("geo", "province", "city", "district")

    // TODO 4. 保存结果
    //    res.show(10,false)
    res.write.parquet("data/dict/geo_dict/output")
    spark.close()
  }

}
