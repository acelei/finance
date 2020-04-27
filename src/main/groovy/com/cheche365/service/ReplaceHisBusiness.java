package com.cheche365.service;

import com.cheche365.entity.DataPool;
import com.cheche365.entity.ReplaceBusiness;
import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * author:WangZhaoliang
 * Date:2020/4/26 19:22
 */
@Service
@Log4j2
public class ReplaceHisBusiness {

    @Autowired
    private Sql baseSql;

    private String listReplaceBusinessBySeGroup = "select * from ( select id, sIds, cIds, insuranceCompany, insuranceCompanyId, province, provinceId, insuranceTypeId, financeOrderDate, orderMonth, policyNo,\n" +
            "       group_concat(id) as ids, sum(sumFee) as sumFee\n" +
            "from (select t1.id,\n" +
            "                      s_id                                  as sIds,\n" +
            "                      c_id                                  as cIds,\n" +
            "                      保险公司                                  as insuranceCompany,\n" +
            "                      保险公司id                                as insuranceCompanyId,\n" +
            "                      省                                     as province,\n" +
            "                      t3.id                                 as provinceId,\n" +
            "                      if(`8-险种名称` = '交强险', 1, 2)            as insuranceTypeId,\n" +
            "                      `9-保单出单日期`                            as financeOrderDate,\n" +
            "                      left(`9-保单出单日期`, 7)                   as orderMonth,\n" +
            "                      `6-保单单号`                                as policyNo,\n" +
            "                      `5-投保人名称` as applicant, `resultTableNameVal` as tableName, \n" +
            "                      ifnull(sum_fee, 0.00) as sumFee\n" +
            "               from `resultTableNameVal` t1\n" +
            "                      left join area t3\n" +
            "                                on t1.省 = t3.name\n" +
            "                      left join (select *\n" +
            "                                 from business_replace_ref\n" +
            "                                 where table_name = 'resultTableNameVal'\n" +
            "               ) t4\n" +
            "                                on t1.id = t4.finance_id\n" +
            "               where handle_sign = 6 and `8-险种名称` in ('交强险', '商业险')\n" +
            "                 and `9-保单出单日期` >= '2019-01-01'\n" +
            "                 and t4.id is null\n" +
            "              ) as temp\n" +
            "group by policyNo, insuranceTypeId, insuranceCompanyId, province) as temp where sumFee < 0";

    private String listReplaceBusinessByCoGroup = "select * from ( select id, sIds, cIds, insuranceCompany, insuranceCompanyId, province, provinceId, insuranceTypeId, financeOrderDate, orderMonth, policyNo, agentName,\n" +
            "       group_concat(id) as ids, sum(sumFee) as sumFee\n" +
            "from (select t1.id,\n" +
            "                      s_id                                  as sIds,\n" +
            "                      c_id                                  as cIds,\n" +
            "                      保险公司                                  as insuranceCompany,\n" +
            "                      保险公司id                                as insuranceCompanyId,\n" +
            "                      省                                     as province,\n" +
            "                      t3.id                                 as provinceId,\n" +
            "                      if(`8-险种名称` = '交强险', 1, 2)            as insuranceTypeId,\n" +
            "                      `9-保单出单日期`                            as financeOrderDate,\n" +
            "                      left(`9-保单出单日期`, 7)                   as orderMonth,\n" +
            "                      `6-保单单号`                                as policyNo,\n" +
            "                      `5-投保人名称` as applicant, \n" +
            "                      `40-代理人名称`                             as agentName, `resultTableNameVal` as tableName, \n" +
            "                      ifnull(sum_commission, 0.00) as sumFee\n" +
            "               from `resultTableNameVal` t1\n" +
            "                      left join area t3\n" +
            "                                on t1.省 = t3.name\n" +
            "                      left join (select *\n" +
            "                                 from business_replace_ref\n" +
            "                                 where table_name = 'resultTableNameVal'\n" +
            "               ) t4\n" +
            "                                on t1.id = t4.finance_id\n" +
            "               where handle_sign = 6 and `8-险种名称` in ('交强险', '商业险')\n" +
            "                 and `9-保单出单日期` >= '2019-01-01'\n" +
            "                 and t4.id is null\n" +
            "              ) as temp\n" +
            " group by policyNo, insuranceTypeId, insuranceCompanyId, province ) as temp where sunFee < 0";

    public void replaceHistoryBusiness(String settlementTableName, String commissionTableName) throws SQLException {
        List<ReplaceBusiness> replaceBusinessList = new ArrayList<>();
        List<GroovyRowResult> settlementList = baseSql.rows(listReplaceBusinessBySeGroup.replaceAll("resultTableNameVal", settlementTableName));
        if (CollectionUtils.isNotEmpty(settlementList)) {
            replaceBusinessList.addAll(ReplaceBusinessData.transMapList2Bean(settlementList));
        }

        List<GroovyRowResult> commissionList = baseSql.rows(listReplaceBusinessByCoGroup.replaceAll("resultTableNameVal", commissionTableName));
        if (CollectionUtils.isNotEmpty(settlementList)) {
            replaceBusinessList.addAll(ReplaceBusinessData.transMapList2Bean(commissionList));
        }

        for (ReplaceBusiness finance : replaceBusinessList) {
            DataPool dataPool = findReplaceData(finance, 1);
            if (dataPool == null) {
                dataPool = findReplaceData(finance, 2);
            }

            if (dataPool == null) {
                dataPool = findReplaceData(finance, 3);
            }

            if (dataPool == null) {
                log.info("==replace failed! resultTableName:{}, financeId:{}", finance.getTableName(), finance.getId());
                continue;
            }

            List<Map<String, Object>> insertMapList = new ArrayList<>();
            for (String id : finance.getIds().split(",")) {
                Map<String, Object> setMap = new HashMap<>();
                setMap.put("tableName", finance.getTableName());
                setMap.put("businessTableName", "das_data_pool_history");
                setMap.put("financeId", id);
                setMap.put("businessData", dataPool);
                insertMapList.add(setMap);
            }
            baseSql.executeInsert(ReplaceBusinessData.insertBusinessRefList(insertMapList));
            baseSql.executeUpdate("update " + finance.getTableName() + " set handle_sign = 9 where id in (" + finance.getIds() + ")");
            log.info("replace success! resultTableName:{}, financeId:{}, businessId:{}", finance.getTableName(), finance.getId(), dataPool.getId());
        }
    }

    private DataPool findReplaceData(ReplaceBusiness finance, int type) throws SQLException {
        String findBusiness = "select id, policy_no as policyNo, insurance_type_id as insuranceTypeId, insurance_company as insuranceCompany, insurance_company_id as insuranceCompanyId, province_id as provinceId, premium, applicant, order_date as orderDate from `das_data_pool_history` where ";
        findBusiness += " insurance_company_id = " + finance.getInsuranceCompanyId() + " and order_date >= '2018-01-01' and premium > " + finance.getSumFee().abs();
        if (type == 1) {
            if (finance.getTableName().startsWith("commission_") && StringUtils.isNotEmpty(finance.getAgentName())) {
                findBusiness += " and agent = '" + finance.getAgentName() + "' " ;
            }
            findBusiness += " and province_id = " + finance.getProvinceId();
        } else if (type == 2) {
            findBusiness += " and province_id = " + finance.getProvinceId();
        }

        findBusiness += " order by order_date desc limit 1";
        GroovyRowResult grr = baseSql.firstRow(findBusiness);
        return ReplaceBusinessData.transMap2Bean(grr);
    }


}
