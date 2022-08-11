package org.example.olap_web.dao;

import org.example.olap_web.pojo.DNUOneDim;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.example.olap_web.utils.PrestoConn.getConnection;

public class DNUDaoImpl extends BaseDao implements DNUDao{
    Connection conn = getConnection();
    // TODO 单维度查询日活数类
    @Override
    public List<DNUOneDim> getOneDimDnuInfo(String dim) {
        List<String> columns = new ArrayList();
        columns.add("province");
        columns.add("city");
        columns.add("district");
        columns.add("appver");
        columns.add("devicetype");
        columns.remove(dim);
        StringBuilder builder = new StringBuilder();
        // 拼接查询语句
        // select province as dim,dnu_cnts from ADS_APL_DNU_CUBE where province is not null and coalesce(city,district,appver,devicetype) is null
        builder.append("select ")
                .append(dim).append(" as dim,dnu_cnts ")
                .append("from ADS_APL_DNU_CUBE where ")
                .append(dim).append(" is not null and coalesce(");
        for (String str:columns){
            builder.append(str).append(",");
        }
        builder.deleteCharAt(builder.length()-1).append(") is null");
        String sql = builder.toString();
        System.out.println(sql);
        List<DNUOneDim> beanList = getBeanList(DNUOneDim.class, conn, sql);
        return beanList;
    }


    public static void main(String[] args) {
        DNUDaoImpl dauDao = new DNUDaoImpl();
        List<DNUOneDim> provinceDAUList = dauDao.getOneDimDnuInfo("province");
        System.out.println(provinceDAUList);
        // 查询结果
        // [DNUOneDim(dim=四川省, dnu_cnts=2), DNUOneDim(dim=宁夏回族自治区, dnu_cnts=1), DNUOneDim(dim=安徽省, dnu_cnts=2), DNUOneDim(dim=山东省, dnu_cnts=1), DNUOneDim(dim=山西省, dnu_cnts=1), DNUOneDim(dim=广东省, dnu_cnts=3), DNUOneDim(dim=新疆维吾尔自治区, dnu_cnts=1), DNUOneDim(dim=未知, dnu_cnts=183), DNUOneDim(dim=江苏省, dnu_cnts=2), DNUOneDim(dim=江西省, dnu_cnts=1), DNUOneDim(dim=河北省, dnu_cnts=2), DNUOneDim(dim=河南省, dnu_cnts=1), DNUOneDim(dim=海南省, dnu_cnts=1), DNUOneDim(dim=湖南省, dnu_cnts=5), DNUOneDim(dim=福建省, dnu_cnts=1), DNUOneDim(dim=西藏自治区, dnu_cnts=2), DNUOneDim(dim=贵州省, dnu_cnts=1), DNUOneDim(dim=辽宁省, dnu_cnts=2), DNUOneDim(dim=陕西省, dnu_cnts=2)]

    }


}
