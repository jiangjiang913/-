package org.example.olap_web.pojo;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
//TODO 流量概括分析ads_apl_tfc_ovw专用bean
public class Traffic {
    String dim;
    Integer pv_amt;
    Integer uv_amt;
    Integer se_amt;
    Double time_avg_se;
    Double time_avg_u;
    Double se_avg_u;
    Double pv_avg_u;
    Double rbu_ratio;
}
