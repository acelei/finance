package com.cheche365.entity;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

/**
 * author:WangZhaoliang
 * Date:2020/4/12 19:36
 */
@Data
public class DataPool {

    private Long id;
    private String policyNo;
    private Long insuranceTypeId;
    private String insuranceCompany;
    private Long insuranceCompanyId;
    private String insuranceType;
    private Long provinceId;
    private BigDecimal premium;
    private String applicant;
    private Date orderDate;
    private Integer isDel;
    private Integer handleSign;

}
