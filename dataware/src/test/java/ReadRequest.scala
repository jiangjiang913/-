import cn.doitedu.commons.util.SparkUtil

object ReadRequest {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)

//    val df = spark.read.parquet("data/dict/geo_dict/output")
    val df = spark.read.parquet("data/idmp/2020-01-11")
//    val df = spark.read.parquet("data/applog_processed/2020-01-11")
    df.show(10,false)
  }

}
