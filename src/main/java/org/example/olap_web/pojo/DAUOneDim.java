package org.example.olap_web.pojo;

import lombok.*;

/**
 * 单维度日新数实体类
 */

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class DAUOneDim {
    String dim;
    Integer dau_cnts;
}
