package org.example.olap_web.pojo;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class Advertise {

    String adid;
    Integer show_ct;
    Integer show_us;
    Integer click_ct;
    Integer click_us;

}
