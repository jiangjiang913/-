package cn.doitedu.dw.pre

import ch.hsr.geohash.GeoHash
import cn.doitedu.commons.util.SparkUtil
import cn.doitedu.dw.beans.AppLogBean
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Dataset

import java.util

/**
 * 数据预处理
 */

object AppLogDataPreprocess {

  def main(args: Array[String]): Unit = {
    // TODO 构建SparkSession
//    val session = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    val session = SparkUtil.getSparkSession(this.getClass.getSimpleName,args(4))   // 传入master
    import session.implicits._

    // TODO 数据预处理   数据加载、数据解析、数据过滤、数据集成等
    // 1. 数据加载————加载当日的app埋点日志数据
//    val ds: Dataset[String] = session.read.textFile("data/yiee_logs/2020-01-11/app")
    val ds: Dataset[String] = session.read.textFile(args(0))
//    ds.show()
    // 2. 数据解析————json 解析，解析成功的返回 LogBean 对象，解析失败的返回 null
    // 3. 数据过滤————对解析后的结果进行过滤，清掉 json 不完整的脏数据，清掉不符合规则的数据
    val res = ds.map(line => {
      var bean:AppLogBean = null
      try {
        val jsonobj = JSON.parseObject(line)
        // (1) 解析并抽取各个字段
        val eventid = jsonobj.getString("eventid")
        val eventobj: JSONObject = jsonobj.getJSONObject("event")
        import scala.collection.JavaConversions._
        val javaMap: util.Map[String, String] = eventobj.getInnerMap.asInstanceOf[util.Map[String, String]]
        val event: Map[String, String] = javaMap.toMap

        val userobj = jsonobj.getJSONObject("user")
        val uid = userobj.getString("uid")
        //        val account = userobj.getString("account")
        //        val email = userobj.getString("email")
        //        val phoneNbr = userobj.getString("phoneNbr")
        //        val birthday = userobj.getString("birthday")
        //        val isRegistered = userobj.getString("isRegistered")
        //        val isLogin = userobj.getString("isLogin")
        //        val addr = userobj.getString("addr")
        //        val gender = userobj.getString("gender")

        val phoneobj = userobj.getJSONObject("phone")
        val imei = phoneobj.getString("imei")
        val mac = phoneobj.getString("mac")
        val imsi = phoneobj.getString("imsi")
        val osName = phoneobj.getString("osName")
        val osVer = phoneobj.getString("osVer")
        val androidId = phoneobj.getString("androidId")
        val resolution = phoneobj.getString("resolution")
        val deviceType = phoneobj.getString("deviceType")
        val deviceId = phoneobj.getString("deviceId")
        val uuid = phoneobj.getString("uuid")

        val appobj = userobj.getJSONObject("app")
        val appid = appobj.getString("appid")
        val appVer = appobj.getString("appVer")
        val release_ch = appobj.getString("release_ch")
        val promotion_ch = appobj.getString("promotion_ch")

        val locobj = userobj.getJSONObject("loc")
        //        val areacode = locobj.getString("areacode")
        val longtitude = locobj.getDouble("longtitude")
        val latitude = locobj.getDouble("latitude")
        val carrier = locobj.getString("carrier")
        val netType = locobj.getString("netType")
        val cid_sn = locobj.getString("cid_sn")
        val ip = locobj.getString("ip")

        val sessionId = userobj.getString("sessionId")

        val timestamp = jsonobj.getString("timestamp").toLong

        // (2) 判断数据是否符合要求，符合要求则返回AppLogBean对象，否则返回null
        //  uid | imei | mac | imsi | androidId | uuid 标识字段不能全为空
        val flagFields = (imei + imsi + mac + uid + uuid + androidId).replaceAll("null", "")
        // event、eventid和sessionId缺一不可
        if (StringUtils.isNotBlank(flagFields) && event != null && StringUtils.isNotBlank(eventid) && StringUtils.isNotBlank(sessionId)) {
          // 将提取出来的各个字段，封装到AppLogBean中
          bean = AppLogBean(
            Long.MinValue,
            eventid,
            event,
            uid,
            imei,
            mac,
            imsi,
            osName,
            osVer,
            androidId,
            resolution,
            deviceType,
            deviceId,
            uuid,
            appid,
            appVer,
            release_ch,
            promotion_ch,
            longtitude,
            latitude,
            carrier,
            netType,
            cid_sn,
            ip,
            sessionId,
            timestamp
          )
        }
      } catch {
        case e: Exception => {
          e.printStackTrace()
        }
      }
      bean
    })
//      .toDF().show()

    // 4. 数据集成————对数据进行字典知识集成
    // 4.1 加载geo地理位置字典，并收集到driver端，然后广播出去
//    val geodf = session.read.parquet("data/dict/geo_dict/output")
    val geodf = session.read.parquet(args(1))
    val geoMap: collection.Map[String, (String, String, String)] = geodf.rdd.map(row => {
      val geo = row.getAs[String]("geo")
      val province = row.getAs[String]("province")
      val city = row.getAs[String]("city")
      val district = row.getAs[String]("district")
      (geo, (province, city, district))
    }).collectAsMap()
    val bc_geo = session.sparkContext.broadcast(geoMap)

    // 4.2 加载id映射字典，并收集到driver端，然后广播出去
//    val idmpdf = session.read.parquet("data/idmp/2020-01-11")
    val idmpdf = session.read.parquet(args(2))
    val idMap = idmpdf.rdd.map(row => {
      val id = row.getAs[Long]("biaoshi_hashcode")
      val guid = row.getAs[Long]("guid")
      (id, guid)
    }).collectAsMap()
    val bc_id = session.sparkContext.broadcast(idMap)

    // 4.3 填充省、市、区和guid
    val bean: Dataset[AppLogBean] = res.filter(_ != null)
    bean.map(bean => {
      val geoDict = bc_geo.value
      val idmpDict = bc_id.value

      // (1) 查geo地域字典，填充省市区
      val lat = bean.latitude
      val lng = bean.longtitude
      val mygeo = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 5)
      val maybeTuple: Option[(String, String, String)] = geoDict.get(mygeo)
      if (maybeTuple.isDefined) {
        val areaNames = maybeTuple.get
        bean.province = areaNames._1
        bean.city = areaNames._2
        bean.district = areaNames._3

      }
      // (2) 查id映射字典，填充guid
      val ids = Array(bean.imei, bean.imsi, bean.mac, bean.androidId, bean.uuid, bean.uid)
      val mouId = ids.filter(StringUtils.isNotBlank(_))(0)
      val maybeLong = idmpDict.get(mouId.hashCode.toLong)
      if (maybeLong.isDefined) {
        val guid = maybeLong.get
        bean.guid = guid
      }
      bean
    }).filter(bean => bean.guid != Long.MinValue)
      .toDF()
//      .show()
      .write
//      .parquet("data/applog_processed/2020-01-11")
      .parquet(args(3))

    // TODO 关闭Session
    session.close()



  }

}
