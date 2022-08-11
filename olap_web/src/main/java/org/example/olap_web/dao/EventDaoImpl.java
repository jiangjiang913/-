package org.example.olap_web.dao;

import org.example.olap_web.pojo.Event;
import org.example.olap_web.pojo.Eventid;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.example.olap_web.utils.PrestoConn.getConnection;

public class EventDaoImpl extends BaseDao implements EventDao {
    Connection conn = getConnection();
    // TODO 交互事件查询类
    @Override
    public List<Event> getEventInfo() {
        String sql = "select eventid,cnt as cnts,users from ADS_APL_ITR_CUBE " +
                "where coalesce(appver,province,city,district,osname,osver,release_ch)is null";
        System.out.println(sql);
        List<Event> beanList = getBeanList(Event.class, conn, sql);
        return beanList;
    }
    // TODO 交互事件用户top10
    @Override
    public List<Eventid> getEventIdInfo(String eventid) {
        Statement stmt = null;
        ResultSet rs = null;
        ArrayList<Eventid> arrayList = new ArrayList<>();
        try {
            stmt = conn.createStatement();
            String sql = "select guid,cnt as cnts from ADS_APL_ITR_TOP100_U where eventid='"+eventid+"' and rn<=10";
            System.out.println(sql);
            rs = stmt.executeQuery(sql);
            while(rs.next()){
                Integer guid = rs.getInt(1);
                Integer cnts = rs.getInt(2);
                Eventid eventid1 = new Eventid(guid, cnts);
                arrayList.add(eventid1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return arrayList;
    }

    public static void main(String[] args) {
        EventDaoImpl eventDao = new EventDaoImpl();
        List<Event> eventList = eventDao.getEventInfo();
        System.out.println(eventList);
        // 查询结果
        // [Event(eventid=accusationEvent, cnts=249, users=128), Event(eventid=appClickEvent, cnts=15999, users=214), Event(eventid=appStartEvent, cnts=4448, users=214), Event(eventid=appviewEvent, cnts=4448, users=214), Event(eventid=clickChannelEvent, cnts=5304, users=213), Event(eventid=commentEvent, cnts=899, users=194), Event(eventid=enterTabEvent, cnts=7630, users=214), Event(eventid=favoriteEvent, cnts=1329, users=206), Event(eventid=getCodeEvent, cnts=219, users=124), Event(eventid=installEvent, cnts=448, users=168), Event(eventid=loginEvent, cnts=94, users=94), Event(eventid=searchEvent, cnts=7226, users=202), Event(eventid=shareEvent, cnts=1361, users=204), Event(eventid=sinUpEvent, cnts=73, users=73), Event(eventid=viewConentDetailEvent, cnts=44918, users=211), Event(eventid=webStayEvent, cnts=24211, users=214)]
        List<Eventid> accusationEventList = eventDao.getEventIdInfo("accusationEvent");
        System.out.println(accusationEventList);
        // 查询结果
        // [Eventid(guid=1640216713, cnts=5), Eventid(guid=1924050218, cnts=5), Eventid(guid=1245099951, cnts=4), Eventid(guid=244684380, cnts=4), Eventid(guid=1833727431, cnts=4), Eventid(guid=-230179586, cnts=4), Eventid(guid=874901522, cnts=4), Eventid(guid=-1191483580, cnts=4), Eventid(guid=-1922384275, cnts=4), Eventid(guid=-1317699520, cnts=4)]

    }
}
