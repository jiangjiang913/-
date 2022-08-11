package org.example.olap_web.dao;

import org.example.olap_web.pojo.DAUOneDim;

import java.util.List;

public interface DAUDao {
    List<DAUOneDim> getOneDimDauInfo(String dim);

}
