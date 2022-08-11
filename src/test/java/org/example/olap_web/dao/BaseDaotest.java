package org.example.olap_web.dao;

import org.example.olap_web.dao.BaseDao;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.example.olap_web.utils.PrestoConn.getConnection;

public class BaseDaotest extends BaseDao {
    Connection connection= getConnection();

}

