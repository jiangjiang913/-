package org.example.olap_web.dao;

import org.example.olap_web.pojo.Traffic;

import java.util.List;

public interface TrafficDao {
    List<Traffic> getDimTrafficInfo(String dim) throws Exception;

}
