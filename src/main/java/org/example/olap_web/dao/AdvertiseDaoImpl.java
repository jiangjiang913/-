package org.example.olap_web.dao;

import org.example.olap_web.pojo.Advertise;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.example.olap_web.utils.PrestoConn.getConnection;

public class AdvertiseDaoImpl extends BaseDao implements AdvertiseDao  {
    @Override
    public List<Advertise> getAdvertiseInfo() {
        Connection conn = getConnection();
        Statement stmt = null;
        ResultSet rs = null;
        ArrayList<Advertise> arrayList = new ArrayList<>();
        try {
            stmt = conn.createStatement();
            String sql = "select adid,show_ct,show_us,click_ct, click_us from ADS_APL_ADV_OVW";
            System.out.println(sql);
            rs = stmt.executeQuery(sql);
            while(rs.next()){
                String adid = rs.getString(1);
                Integer show_ct = rs.getInt(2);
                Integer show_us = rs.getInt(3);
                Integer click_ct = rs.getInt(4);
                Integer click_us = rs.getInt(5);
                Advertise advertise = new Advertise(adid,show_ct,show_us,click_ct,click_us);
                arrayList.add(advertise);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return arrayList;
    }

    public static void main(String[] args) {
        AdvertiseDaoImpl advertiseDao = new AdvertiseDaoImpl();
        List<Advertise> advertiseList = advertiseDao.getAdvertiseInfo();
        System.out.println(advertiseList);
        // 查询结果
        // [Event(eventid=accusationEvent, cnts=249, users=128), Event(eventid=appClickEvent, cnts=15999, users=214), Event(eventid=appStartEvent, cnts=4448, users=214), Event(eventid=appviewEvent, cnts=4448, users=214), Event(eventid=clickChannelEvent, cnts=5304, users=213), Event(eventid=commentEvent, cnts=899, users=194), Event(eventid=enterTabEvent, cnts=7630, users=214), Event(eventid=favoriteEvent, cnts=1329, users=206), Event(eventid=getCodeEvent, cnts=219, users=124), Event(eventid=installEvent, cnts=448, users=168), Event(eventid=loginEvent, cnts=94, users=94), Event(eventid=searchEvent, cnts=7226, users=202), Event(eventid=shareEvent, cnts=1361, users=204), Event(eventid=sinUpEvent, cnts=73, users=73), Event(eventid=viewConentDetailEvent, cnts=44918, users=211), Event(eventid=webStayEvent, cnts=24211, users=214)]

    }
}
