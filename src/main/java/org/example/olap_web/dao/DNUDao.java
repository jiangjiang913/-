package org.example.olap_web.dao;

import org.example.olap_web.pojo.DNUOneDim;

import java.util.List;
//TODO 日新数接口
interface DNUDao {
    List<DNUOneDim> getOneDimDnuInfo(String dim);
}
