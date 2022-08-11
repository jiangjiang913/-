package org.example.olap_web.dao;

import org.example.olap_web.pojo.DAUOneDim;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.example.olap_web.utils.PrestoConn.getConnection;

public class DAUDaoImpl extends BaseDao implements DAUDao{
    Connection conn = getConnection();
    // TODO 单维度查询日活数类
    @Override
    public List<DAUOneDim> getOneDimDauInfo(String dim) {
        List<String> columns = new ArrayList();
        columns.add("province");
        columns.add("city");
        columns.add("district");
        columns.add("appver");
        columns.add("devicetype");
        columns.remove(dim);
        StringBuilder builder = new StringBuilder();
        // 拼接查询语句
        // select province,dau_cnts from ADS_APL_DAU_CUBE where province is not null and coalesce(city,district,appver,devicetype) is null;
        builder.append("select ")
                .append(dim).append(" as dim,dau_cnts ")
                .append("from ADS_APL_DAU_CUBE where ")
                .append(dim).append(" is not null and coalesce(");
        for (String str:columns){
            builder.append(str).append(",");
        }
        builder.deleteCharAt(builder.length()-1).append(") is null");
        String sql = builder.toString();
        System.out.println(sql);
        List<DAUOneDim> beanList = getBeanList(DAUOneDim.class, conn, sql);
        return beanList;
    }


    public static void main(String[] args) {
        DAUDaoImpl dauDao = new DAUDaoImpl();
        List<DAUOneDim> provinceDAUList = dauDao.getOneDimDauInfo("province");
        System.out.println(provinceDAUList);
    }

    // 得到查询结果
    // [DAUOneDim(dim=四川省, dau_cnts=2), DAUOneDim(dim=宁夏回族自治区, dau_cnts=1), DAUOneDim(dim=安徽省, dau_cnts=2), DAUOneDim(dim=山东省, dau_cnts=1), DAUOneDim(dim=山西省, dau_cnts=1), DAUOneDim(dim=广东省, dau_cnts=3), DAUOneDim(dim=新疆维吾尔自治区, dau_cnts=1), DAUOneDim(dim=未知, dau_cnts=183), DAUOneDim(dim=江苏省, dau_cnts=2), DAUOneDim(dim=江西省, dau_cnts=1), DAUOneDim(dim=河北省, dau_cnts=2), DAUOneDim(dim=河南省, dau_cnts=1), DAUOneDim(dim=海南省, dau_cnts=1), DAUOneDim(dim=湖南省, dau_cnts=5), DAUOneDim(dim=福建省, dau_cnts=1), DAUOneDim(dim=西藏自治区, dau_cnts=2), DAUOneDim(dim=贵州省, dau_cnts=1), DAUOneDim(dim=辽宁省, dau_cnts=2), DAUOneDim(dim=陕西省, dau_cnts=2)]

}
