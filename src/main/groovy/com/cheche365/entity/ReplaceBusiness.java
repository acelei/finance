package com.cheche365.entity;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Date;

/**
 * author:WangZhaoliang
 * Date:2020/4/1 15:49
 */
@Data
public class ReplaceBusiness {

    private Long id;

    private String ids;

    private String sids;

    private String cids;

    private Long insuranceCompanyId;

    private String insuranceCompany;

    private Long provinceId;

    private String insuranceType;

    private Long insuranceTypeId;

    private BigDecimal sumFee;

    private BigDecimal sumCommission;

    private String applicant;

    private Date financeOrderDate;

    private String orderMonth;

    private String agentName;

    private String policyNo;

}
