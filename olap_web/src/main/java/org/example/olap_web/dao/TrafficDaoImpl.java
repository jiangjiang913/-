package org.example.olap_web.dao;

import org.example.olap_web.pojo.Traffic;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.example.olap_web.utils.PrestoConn.getConnection;

public class TrafficDaoImpl extends BaseDao implements TrafficDao{
    Connection conn = null;
    List<Traffic> trafficlist;
    // TODO 单维度流量概况分析类
    @Override
    public List getDimTrafficInfo(String dim) {
        try {
            conn = getConnection();
            List<String> list = new ArrayList();
            list.add("province");
            list.add("city");
            list.add("district");
            list.add("devicetype");
            list.add("osname");
            list.add("osver");
            list.add("release_ch");
            list.add("promotion_ch");
            list.remove(dim);
            StringBuilder stringBuilder = new StringBuilder();
            // 拼接查询语句
            stringBuilder.append("select ").append(dim).append(" as dim,");
            stringBuilder.append("pv_amt").append(",");
            stringBuilder.append("uv_amt").append(",");
            stringBuilder.append("se_amt").append(",");
            stringBuilder.append("time_avg_se").append(",");
            stringBuilder.append("time_avg_u").append(",");
            stringBuilder.append("se_avg_u").append(",");
            stringBuilder.append("pv_avg_u").append(",");
            stringBuilder.append("rbu_ratio ")
                    .append("from ads_apl_tfc_ovw where dt='2022-06-15' and ")
                    .append(dim).append(" is not null and coalesce(");
            for (String str : list) {
                stringBuilder.append(str).append(",");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            stringBuilder.append(") is null");
            String sql = stringBuilder.toString();
            trafficlist = getBeanList(Traffic.class, conn, sql);

        }finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return trafficlist;
    }

    public static void main(String[] args) {
        TrafficDaoImpl trafficDao = new TrafficDaoImpl();
        List listProvinceInfo = trafficDao.getDimTrafficInfo("province");
        for(Object i : listProvinceInfo){
            System.out.println(i);
        }

        // 查询结果
        // Traffic(dim=四川省, pv_amt=49, uv_amt=2, se_amt=2, time_avg_se=7.50345E7, time_avg_u=7.50345E7, se_avg_u=1.0, pv_avg_u=24.5, rbu_ratio=0.0)
        // Traffic(dim=宁夏回族自治区, pv_amt=13, uv_amt=1, se_amt=1, time_avg_se=3.6142E7, time_avg_u=3.6142E7, se_avg_u=1.0, pv_avg_u=13.0, rbu_ratio=0.0)
        // Traffic(dim=安徽省, pv_amt=31, uv_amt=2, se_amt=2, time_avg_se=4.26345E7, time_avg_u=4.26345E7, se_avg_u=1.0, pv_avg_u=15.5, rbu_ratio=0.0)
        // Traffic(dim=山东省, pv_amt=30, uv_amt=1, se_amt=1, time_avg_se=8.1221E7, time_avg_u=8.1221E7, se_avg_u=1.0, pv_avg_u=30.0, rbu_ratio=0.0)
        // Traffic(dim=山西省, pv_amt=9, uv_amt=1, se_amt=1, time_avg_se=2.0281E7, time_avg_u=2.0281E7, se_avg_u=1.0, pv_avg_u=9.0, rbu_ratio=0.0)
        // Traffic(dim=广东省, pv_amt=42, uv_amt=3, se_amt=3, time_avg_se=4.2401E7, time_avg_u=4.2401E7, se_avg_u=1.0, pv_avg_u=14.0, rbu_ratio=0.0)
        // Traffic(dim=新疆维吾尔自治区, pv_amt=30, uv_amt=1, se_amt=1, time_avg_se=8.1733E7, time_avg_u=8.1733E7, se_avg_u=1.0, pv_avg_u=30.0, rbu_ratio=0.0)
        // Traffic(dim=未知, pv_amt=3733, uv_amt=183, se_amt=183, time_avg_se=5.6240912568306014E7, time_avg_u=5.6240912568306014E7, se_avg_u=1.0, pv_avg_u=20.398907103825138, rbu_ratio=0.0)
        // Traffic(dim=江苏省, pv_amt=63, uv_amt=2, se_amt=2, time_avg_se=9.46645E7, time_avg_u=9.46645E7, se_avg_u=1.0, pv_avg_u=31.5, rbu_ratio=0.0)
        // Traffic(dim=江西省, pv_amt=40, uv_amt=1, se_amt=1, time_avg_se=1.22747E8, time_avg_u=1.22747E8, se_avg_u=1.0, pv_avg_u=40.0, rbu_ratio=0.0)
        // Traffic(dim=河北省, pv_amt=46, uv_amt=2, se_amt=2, time_avg_se=6.96265E7, time_avg_u=6.96265E7, se_avg_u=1.0, pv_avg_u=23.0, rbu_ratio=0.0)
        // Traffic(dim=河南省, pv_amt=19, uv_amt=1, se_amt=1, time_avg_se=5.5854E7, time_avg_u=5.5854E7, se_avg_u=1.0, pv_avg_u=19.0, rbu_ratio=0.0)
        // Traffic(dim=海南省, pv_amt=39, uv_amt=1, se_amt=1, time_avg_se=1.2326E8, time_avg_u=1.2326E8, se_avg_u=1.0, pv_avg_u=39.0, rbu_ratio=0.0)
        // Traffic(dim=湖南省, pv_amt=100, uv_amt=5, se_amt=5, time_avg_se=5.50484E7, time_avg_u=5.50484E7, se_avg_u=1.0, pv_avg_u=20.0, rbu_ratio=0.0)
        // Traffic(dim=福建省, pv_amt=3, uv_amt=1, se_amt=1, time_avg_se=3314000.0, time_avg_u=3314000.0, se_avg_u=1.0, pv_avg_u=3.0, rbu_ratio=0.0)
        // Traffic(dim=西藏自治区, pv_amt=68, uv_amt=2, se_amt=2, time_avg_se=9.13865E7, time_avg_u=9.13865E7, se_avg_u=1.0, pv_avg_u=34.0, rbu_ratio=0.0)
        // Traffic(dim=贵州省, pv_amt=10, uv_amt=1, se_amt=1, time_avg_se=2.2364E7, time_avg_u=2.2364E7, se_avg_u=1.0, pv_avg_u=10.0, rbu_ratio=0.0)
        // Traffic(dim=辽宁省, pv_amt=29, uv_amt=2, se_amt=2, time_avg_se=4.03455E7, time_avg_u=4.03455E7, se_avg_u=1.0, pv_avg_u=14.5, rbu_ratio=0.0)
        // Traffic(dim=陕西省, pv_amt=30, uv_amt=2, se_amt=2, time_avg_se=5.59435E7, time_avg_u=5.59435E7, se_avg_u=1.0, pv_avg_u=15.0, rbu_ratio=0.0)
    }



}
