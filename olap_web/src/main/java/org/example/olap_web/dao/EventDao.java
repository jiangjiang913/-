package org.example.olap_web.dao;

import org.example.olap_web.pojo.Event;
import org.example.olap_web.pojo.Eventid;

import java.util.List;

public interface EventDao {
    List<Event> getEventInfo();
    List<Eventid> getEventIdInfo(String eventid);
}
