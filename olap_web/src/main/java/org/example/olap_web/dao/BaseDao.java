package org.example.olap_web.dao;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public abstract class BaseDao {

    /**
     * 查询操作
     */

    public <T> T getBean(Class<T> clazz,Connection conn,String sql,Object...args) {
        PreparedStatement ps=null;
        ResultSet rs=null;
        T t = null;
        try {
            ps=conn.prepareStatement(sql);
//			填充占位符
            for(int i=0;i<args.length;i++){
                ps.setObject(i+1, args[i]);
            }
//		执行SQL语句，获取结果集
            rs = ps.executeQuery();
//		获取结果数据的元数据
            ResultSetMetaData rsmd = rs.getMetaData();
//		获取结果集的字段数
            int columnCount = rsmd.getColumnCount();
            if(rs.next()){
                t = clazz.newInstance();
                for(int i=0;i<columnCount;i++){
//				获取列名
                    String columnLabel = rsmd.getColumnLabel(i+1);
                    System.out.println(columnLabel);
//				获取列值
                    Object columnValue = rs.getObject(i+1);
//				通过反射，获取User对象的属性，通过属性创建对象
                    Field field = clazz.getDeclaredField(columnLabel);
                    field.setAccessible(true);
                    field.set(t, columnValue);
                }
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return t;
    }



    /**
     * 查询列表操作
     * @throws SQLException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws SecurityException
     * @throws NoSuchFieldException
     */
    public <T> List<T> getBeanList(Class<T> clazz,Connection conn,String sql,Object...args) {
        PreparedStatement ps=null;
        ArrayList ls = null;
        ResultSet rs=null;
        try {
            ps=conn.prepareStatement(sql);
//		填充占位符
            for(int i=0;i<args.length;i++){
                ps.setObject(i+1, args[i]);
            }

//		获得结果集
            rs=ps.executeQuery();
//		获得结果集的元数据
            ResultSetMetaData rsmd = rs.getMetaData();
//		获取结果集的字段数
            int columnCount = rsmd.getColumnCount();
            ls = new ArrayList();
            while(rs.next()){
                T t = clazz.newInstance();
                for(int i=0;i<columnCount;i++){
//				获取字段名
                    String columnLabel = rsmd.getColumnLabel(i+1);
                    Object columnValue = rs.getObject(i+1);
//				通过反射设置对象属性和字段值对应
                    Field fields = clazz.getDeclaredField(columnLabel);
//				关闭访问检查
                    /**
                     * AccessibleObject 类是 Field、Method 和 Constructor 对象的基类。它提供了将反射的对象标记为在使用
                     * 时取消默认 Java 语言访问控制检查的能力。对于公共成员、默认（打包）访问成员、受保护成员和私有成员，
                     * 在分别使用 Field、Method 或 Constructor 对象来设置或获取字段、调用方法，或者创建和初始化类的新实例
                     * 的时候，会执行访问检查。
                     */
                    fields.setAccessible(true);
//				给属性赋值
                    fields.set(t, columnValue);
                }
                ls.add(t);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return ls;
    }


    //	其他聚合查询操作
    public <E> E getValue(Class<E> clazz,Connection conn,String sql,Object...args) {
        PreparedStatement ps=null;
        ResultSet rs=null;
        E e1 = null;
//		预编译SQL语句
        try {
            ps=conn.prepareStatement(sql);
//		填充占位符
            for(int i=0;i<args.length;i++){
                ps.setObject(i+1, args[i]);
            }
//		执行SQL语句，获得结果
            rs = ps.executeQuery();
            if(rs.next()){
                e1 = (E) rs.getObject(1);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return e1;

    }

}
