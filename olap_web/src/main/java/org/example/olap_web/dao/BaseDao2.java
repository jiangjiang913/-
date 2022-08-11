package org.example.olap_web.dao;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class BaseDao2 {
    QueryRunner runner = new QueryRunner();
    //TODO 查询一个bean
    public <T> T getBean(Class<T> clazz, Connection conn,String sql,Object...args)  {
        T t = null;
        try {
            t = clazz.newInstance();
            BeanHandler<T> handler = new BeanHandler<>(clazz);
            t = runner.query(conn, sql, handler, args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return t;
    }
    //TODO 查询list<Bean>
    public <T> List<T> getBeanList(Class<T> clazz,Connection conn,String sql,Object...args) {
        BeanListHandler<T> listHandler = new BeanListHandler<T>(clazz);
        List<T> list= new ArrayList<T>();
        try {
            list = runner.query(conn, sql, listHandler, args);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }
    //TODO 查询单值
    public Object getValue(Class clazz,Connection conn,String sql,Object...args) {
        Object o = null;
        try {
            ScalarHandler<Object> objectScalarHandler = new ScalarHandler<>();
            o = runner.query(conn, sql, objectScalarHandler, args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return o;
    }
}
