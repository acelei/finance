package com.cheche365.service;

import org.springframework.stereotype.Service;

/**
 * author:WangZhaoliang
 * Date:2020/4/28 13:31
 */
@Service("replaceUnAutoBusiness")
public class ReplaceUnAutoBusiness extends ReplaceHisBusiness {

    private boolean findHistory = false;

    private String findBusinessSql = "select t1.id, `6-保单单号` as policyNo, `8-险种名称` as insuranceType, 保险公司 as insuranceCompany, 保险公司id as insuranceCompanyId, t2.id as provinceId, `11-净保费` as premium, `5-投保人名称` as applicant, `9-保单出单日期` as orderDate from `tableNameVal` t1 left join area t2 on t1.`省` = t2.name where " +
            " `保险公司id` = insuranceCompanyIdVal and `11-净保费` > premiumVal";

    @Override
    public boolean isFindHistory() {
        return findHistory;
    }

    @Override
    public String getFindBusinessSql() {
        return findBusinessSql;
    }
}
