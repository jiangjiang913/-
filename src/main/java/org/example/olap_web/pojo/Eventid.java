package org.example.olap_web.pojo;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class Eventid {
    Integer guid;
    Integer cnts;    // 事件次数
}
