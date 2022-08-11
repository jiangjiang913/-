package org.example.olap_web.pojo;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class Event {
    String eventid;
    Integer cnts;    // 事件次数
    Integer users;  // 事件人数
}
