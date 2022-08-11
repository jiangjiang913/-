package org.example.olap_web.pojo;


import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
//TODO 用于日活多维分析ADS_APL_DAU_CUBE的bean
public class DNUOneDim {
    String dim;
    Integer dnu_cnts;
}
