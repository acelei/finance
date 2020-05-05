package com.cheche365.entity;

import lombok.Data;

import java.math.BigDecimal;

/**
 * author:WangZhaoliang
 * Date:2020/4/9 15:21
 */
@Data
public class TwoHandleSign {

    private Long id;
    private String sids;
    private String cids;
    private String insuranceType;
    private Long insuranceCompanyId;
    private String province;
    private String agentName;
    private String orderMonth;
    private BigDecimal fee;
    private BigDecimal commission;
    private BigDecimal grossMargin;
    private Integer rFlag;

}
