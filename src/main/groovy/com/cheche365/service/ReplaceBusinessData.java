package com.cheche365.service;

import com.cheche365.entity.DataPool;
import com.cheche365.entity.ReplaceBusiness;
import com.cheche365.util.ThreadPoolUtils;
import com.google.common.base.Joiner;
import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * author:WangZhaoliang
 * Date:2020/4/12 18:56
 */
@Log4j2
@Service
public class ReplaceBusinessData {

    @Autowired
    protected Sql baseSql;

    private static final BigDecimal JQ_MIN_FEE_RATE = new BigDecimal(0.04);
    private static final BigDecimal JQ_MAX_FEE_RATE = new BigDecimal(0.08);
    private static final BigDecimal SY_MIN_FEE_RATE = new BigDecimal(0.12);
    private static final BigDecimal SY_MAX_FEE_RATE = new BigDecimal(0.70);
    private static final BigDecimal[] SY_PREMIUM_RATE = new BigDecimal[]{new BigDecimal(0.1887), new BigDecimal(0.1698), new BigDecimal(0.1415)};
    //人保  财险
    private static final Long RENBAO = 2005L;

    private ExecutorCompletionService runCompletionPool = ThreadPoolUtils.getCompletionRunPool();

    private String allInsPro = "select concat(insurance_company_id, '_', province_id) as insPro\n" +
            "      from das_data_pool_business\n" +
            "      group by insurance_company_id, province_id";

    private String updateTableType = "update table_type set flag = 3 where type = 'typeVal'";
    private String updateEigntHandleSign = "update `tableNameVal` set handle_sign = 8 where id in (idListVal)";
    private String updateBusinessDataHandleSign = "update `tableName` set handle_sign = '2' where id = idVal";
    private String updateFinishHandleSign = "update `tableName` set handle_sign = 9 where id in (idList)";
    private String listReplaceBusiness = "select t1.id,\n" +
            "       t1.s_id                                                                                          as sids,\n" +
            "       t1.c_id                                                                                          as cids,\n" +
            "       保险公司                                                                                       as insuranceCompany,\n" +
            "       t2.id                                                                                         as insuranceCompanyId,\n" +
            "       省                                                                                            as province,\n" +
            "       t3.id                                                                                         as provinceId,\n" +
            "       if (`8-险种名称` = '交强险', 1, 2)                                                              as insuranceTypeId,\n" +
            "       `9-保单出单日期`                                                                                as financeOrderDate,\n" +
            "       left(`9-保单出单日期`, 7)                                                                       as orderMonth, \n" +
            "       `6-保单单号` as policyNo, `5-投保人名称` as applicant, \n" +
            "       if(t1.s_id is not null,\n" +
            "          ifnull(`sum_fee`, 0.00),\n" +
            "          ifnull(`sum_commission`, 0.00)\n" +
            "         )                        as sumFee\n" +
            "        from `resultTableNameVal` t1\n" +
            "       left join insurance_company t2\n" +
            "                 on t1.保险公司 = t2.name\n" +
            "       left join area t3\n" +
            "                 on t1.省 = t3.name\n" +
            "       left join (select * from business_replace_ref\n" +
            "         where result_table_name = 'resultTableNameVal'\n" +
            "         ) t4\n" +
            "                 on t1.id = t4.result_id\n" +
            "       where handle_sign in (3,6) \n" +
            "       and `8-险种名称` in ('交强险', '商业险') and `9-保单出单日期` >= '2019-01-01' \n" +
            "       and t4.id is null";

    private String listReplaceBusinessBySe = "select t1.id,\n" +
            "       s_id                                                                                          as sids,\n" +
            "       c_id                                                                                          as cids,\n" +
            "       保险公司                                                                                       as insuranceCompany,\n" +
            "       保险公司id                                                                                     as insuranceCompanyId,\n" +
            "       省                                                                                            as province,\n" +
            "       t3.id                                                                                         as provinceId,\n" +
            "       if (`8-险种名称` = '交强险', 1, 2)                                                              as insuranceTypeId,\n" +
            "       `9-保单出单日期`                                                                                as financeOrderDate,\n" +
            "       left(`9-保单出单日期`, 7)                                                                       as orderMonth,\n" +
            "       `6-保单单号` as policyNo, `5-投保人名称` as applicant, \n" +
            "       ifnull(`sum_fee`, 0.00)                                              as sumFee\n" +
            "        from `resultTableNameVal` t1\n" +
            "       left join area t3\n" +
            "                 on t1.省 = t3.name\n" +
            "       left join (select * from business_replace_ref\n" +
            "         where table_name = 'resultTableNameVal'\n" +
            "         ) t4\n" +
            "       on t1.id = t4.finance_id\n" +
            "       where handle_sign = 6 \n" +
            "       and `8-险种名称` in ('交强险', '商业险') and `sum_fee` >= 0 and `9-保单出单日期` >= '2019-01-01' \n" +
            "       and t4.id is null";

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
            "                      `5-投保人名称` as applicant, \n" +
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
            "group by policyNo, insuranceTypeId, insuranceCompanyId, province, orderMonth) as temp where sumFee >= 0";

    private String listReplaceBusinessByCo = "select t1.id,\n" +
            "       s_id                                                                                          as sids,\n" +
            "       c_id                                                                                          as cids,\n" +
            "       保险公司                                                                                       as insuranceCompany,\n" +
            "       保险公司id                                                                                     as insuranceCompanyId,\n" +
            "       省                                                                                            as province,\n" +
            "       t3.id                                                                                         as provinceId,\n" +
            "       if (`8-险种名称` = '交强险', 1, 2)                                                              as insuranceTypeId,\n" +
            "       `9-保单出单日期`                                                                                as financeOrderDate,\n" +
            "       left(`9-保单出单日期`, 7)                                                                       as orderMonth,\n" +
            "       `6-保单单号` as policyNo, `5-投保人名称` as applicant, \n" +
            "       ifnull(`sum_commission`, 0.00)          as sumFee\n" +
            "        from `resultTableNameVal` t1\n" +
            "       left join area t3\n" +
            "                 on t1.省 = t3.name\n" +
            "       left join (select * from business_replace_ref\n" +
            "         where table_name = 'resultTableNameVal'\n" +
            "         ) t4\n" +
            "       on t1.id = t4.finance_id\n" +
            "       where handle_sign = 6 \n" +
            "       and `8-险种名称` in ('交强险', '商业险') and `sum_commission` >= 0 and `9-保单出单日期` >= '2019-01-01' \n" +
            "       and t4.id is null";

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
            "                      `40-代理人名称`                             as agentName,\n" +
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
            "group by policyNo, insuranceTypeId, insuranceCompanyId, province, orderMonth, agentName ) as temp where sumFee >= 0";

    private String updateSixHandleSignSee = "select *\n" +
            "from (\n" +
            "       select `6-保单单号` as policyNo, count(0) as count, group_concat(id) as ids, sum(sum_fee) as sumFee\n" +
            "       from `settlementTableNameVal`\n" +
            "       where handle_sign = 6\n" +
            "         and `8-险种名称` in ('交强险', '商业险')\n" +
            "         and left(`9-保单出单日期`, 4) = '2019'\n" +
            "       group by `6-保单单号`) as temp\n" +
            "where sumFee = 0";

    private String updateSixHandleSignCo = "select *\n" +
            "from (\n" +
            "       select `6-保单单号` as policyNo, count(0) as count, group_concat(id) as ids, sum(sum_commission) as sumFee\n" +
            "       from `commissionTableNameVal`\n" +
            "       where handle_sign = 6\n" +
            "         and `8-险种名称` in ('交强险', '商业险')\n" +
            "         and left(`9-保单出单日期`, 4) = '2019'\n" +
            "       group by `6-保单单号`) as temp\n" +
            "where sumFee = 0";

    private String updateThreeHandleSignFee = "update `settlementTableName` t1\n" +
            "inner join `resultTableName` t2\n" +
            "on t1.`6-保单单号` = t2.`6-保单单号`\n" +
            "and t1.`8-险种名称` = t2.`8-险种名称`\n" +
            "set t2.handle_sign = 3\n" +
            "where t1.handle_sign = 6\n" +
            "and t1.`8-险种名称` in ('交强险', '商业险')\n" +
            "and left(t1.`9-保单出单日期`, 4) = '2019'\n" +
            "and t1.sum_fee > 0";

    private String updateThreeHandleSignCom = "update `commissionTableName` t1\n" +
            "inner join `resultTableName` t2\n" +
            "on t1.`6-保单单号` = t2.`6-保单单号`\n" +
            "and t1.`8-险种名称` = t2.`8-险种名称`\n" +
            "set t2.handle_sign = 3\n" +
            "where t1.handle_sign = 6\n" +
            "and t1.`8-险种名称` in ('交强险', '商业险')\n" +
            "and left(t1.`9-保单出单日期`, 4) = '2019'\n" +
            "and t1.sum_commission > 0";

    private String updateFinishHandleSignFee = "update `settlementTableName` t1\n" +
            "set handle_sign = 9 \n" +
            "where t1.`8-险种名称` in ('交强险', '商业险')\n" +
            "and left(t1.`9-保单出单日期`, 4) = '2019'\n" +
            "and t1.sum_fee > 0";

    private String updateFinishHandleSignCom = "update `commissionTableName` t1\n" +
            "set handle_sign = 9 \n" +
            "where t1.`8-险种名称` in ('交强险', '商业险')\n" +
            "and left(t1.`9-保单出单日期`, 4) = '2019'\n" +
            "and t1.sum_commission > 0";

    private DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    public void replaceBusinessList(String tableNameRef) {
        try {
            //将散表中的收入或者成本相加为0的数据更新handle_sign=8
            updateScatterFlag(tableNameRef);
            List<GroovyRowResult> grsInsProList = baseSql.rows(allInsPro);
            if (CollectionUtils.isEmpty(grsInsProList)) {
                return;
            }
            List<String> insProList = grsInsProList.stream().map(it -> it.get("insPro").toString()).collect(Collectors.toList());
            String settlementTableName = "settlement_" + tableNameRef;
            replaceBusiness(settlementTableName, insProList, 2);
            replaceBusiness(settlementTableName, insProList, 1);
            String commissionTableName = "commission_" + tableNameRef;
            replaceBusiness(commissionTableName, insProList, 2);
            replaceBusiness(commissionTableName, insProList, 1);
            //将散表中数据推到result中设置handlesign=3
            String resultTableName = "result_" + tableNameRef + "_2";
            setResultThreeHs(commissionTableName, settlementTableName, resultTableName);
            //替换handle_sign为3的数据
            replaceBusiness(resultTableName, insProList, 1);
            log.info("replaceBusiness success! tableNameRef:{}", tableNameRef);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setResultThreeHs(String commissionTableName, String settlementTableName, String resultTableName) throws SQLException {
        baseSql.executeUpdate(updateThreeHandleSignFee.replace("settlementTableName", settlementTableName).replace("resultTableName", resultTableName));
        baseSql.executeUpdate(updateFinishHandleSignFee.replace("settlementTableName", settlementTableName));
        baseSql.executeUpdate(updateThreeHandleSignCom.replace("commissionTableName", commissionTableName).replace("resultTableName", resultTableName));
        baseSql.executeUpdate(updateFinishHandleSignCom.replace("commissionTableName", commissionTableName));
    }

    private void updateScatterFlag(String tableNameRef) throws SQLException {
        String settlementTableName = "settlement_" + tableNameRef;
        String commissionTableName = "commission_" + tableNameRef;
        List<GroovyRowResult> sixSeeList = baseSql.rows(updateSixHandleSignSee.replace("settlementTableNameVal", settlementTableName));
        if (CollectionUtils.isNotEmpty(sixSeeList)) {
            List<String> idList = new ArrayList<>();
            for (GroovyRowResult groovyRowResult : sixSeeList) {
                idList.addAll(Arrays.asList(groovyRowResult.get("ids").toString().split(",")));
            }
            baseSql.executeUpdate(updateEigntHandleSign.replace("tableNameVal", settlementTableName).replace("idListVal", Joiner.on(",").join(idList)));
        }

        List<GroovyRowResult> sixCoList = baseSql.rows(updateSixHandleSignCo.replace("commissionTableNameVal", commissionTableName));
        if (CollectionUtils.isNotEmpty(sixCoList)) {
            List<String> idList = new ArrayList<>();
            for (GroovyRowResult groovyRowResult : sixCoList) {
                idList.addAll(Arrays.asList(groovyRowResult.get("ids").toString().split(",")));
            }
            baseSql.executeUpdate(updateEigntHandleSign.replace("tableNameVal", commissionTableName).replace("idListVal", Joiner.on(",").join(idList)));
        }
    }

    private String replaceBusiness(String resultTableName, List<String> allInsPro, int type) throws SQLException {
        List<ReplaceBusiness> businessList = replaceBusinessListByTableName(resultTableName, type);
        List<Future> futureList = new ArrayList<>();
        for (ReplaceBusiness finance : businessList) {
            String insPro = finance.getInsuranceCompanyId() + "_" + finance.getProvinceId();
            if (!allInsPro.contains(insPro) || finance.getSumFee().compareTo(BigDecimal.ZERO) == 0) {
                continue;
            }
            Future future = runCompletionPool.submit(() -> updateReplaceData(finance, resultTableName, type));
            futureList.add(future);
        }
        for (Future future : futureList) {
            try {
                future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        log.info("replaceBusiness success! resultTableName: {}, type:{}", resultTableName, type);
        return "success";
    }

    private String updateReplaceData(ReplaceBusiness finance, String resultTableName, int type) throws SQLException {
        synchronized (("run_thread_" + finance.getInsuranceCompanyId() + "_" + finance.getProvinceId()).intern()) {
            BigDecimal minPremium, maxPremium;
            String insuranceCompanyTableName = generInsuranceCompany(finance.getInsuranceCompanyId(), finance.getProvinceId());
            //交强
            if (finance.getInsuranceTypeId().equals(1L)) {
                minPremium = finance.getSumFee().abs().divide(JQ_MAX_FEE_RATE, 2, RoundingMode.HALF_UP);
                maxPremium = finance.getSumFee().abs().divide(JQ_MIN_FEE_RATE, 2, RoundingMode.HALF_UP);
            } else {
                minPremium = finance.getSumFee().abs().divide(SY_MAX_FEE_RATE, 2, RoundingMode.HALF_UP);
                maxPremium = finance.getSumFee().abs().divide(SY_MIN_FEE_RATE, 2, RoundingMode.HALF_UP);
            }
            boolean continueLoop = true;
            while (continueLoop) {
                LocalDate startDate = getBusinessStartDate(date2LocalDate(finance.getFinanceOrderDate()));
                String startDateStr = startDate.format(fmt);
                String getReplaceData = "select id, policy_no as policyNo, insurance_type_id as insuranceTypeId, insurance_company as insuranceCompany, insurance_company_id as insuranceCompanyId, province_id as provinceId, premium, applicant, order_date as orderDate from " + insuranceCompanyTableName
                      +  " where province_id = " + finance.getProvinceId() + " and order_date > '" + startDateStr + "' ";
                if (finance.getFinanceOrderDate() != null) {
                    getReplaceData += " and order_date <= '" + formatter.format(finance.getFinanceOrderDate()) + "' ";
                }
                getReplaceData += " and premium >= '" + minPremium + "' and premium <= '" + maxPremium + "' and handle_sign = 0 order by order_date desc, id desc limit 1";
                GroovyRowResult dataPoolList = baseSql.firstRow(getReplaceData);
                DataPool businessData;
                if (dataPoolList == null || dataPoolList.size() == 0) {
                    if (resultTableName.startsWith("result_")) {
                        businessData = generDataPool(finance);
                    } else {
                        break;
                    }
                } else {
                    businessData = transMap2Bean(dataPoolList);
                    String getReplaceBusinessId = "select business_id as businessId from business_replace_ref where business_id = " + businessData.getId() + " limit 1";
                    GroovyRowResult businessId = baseSql.firstRow(getReplaceBusinessId);
                    if (businessId != null && businessId.get("businessId") != null) {
                        log.error("has use repeat businessData! financeId:{}, businessId:{}", finance.getId(), businessData.getId());
                        continue;
                    }
                }

                if (businessData == null) {
                    break;
                }
                if (finance.getSumFee().compareTo(BigDecimal.ZERO) < 0) {
                    businessData.setPremium(BigDecimal.ZERO.subtract(businessData.getPremium()));
                }
                insertBusinessRef(resultTableName, finance, businessData, type);
                baseSql.executeUpdate(updateBusinessDataHandleSign.replace("tableName", insuranceCompanyTableName).replace("idVal", businessData.getId().toString()));
                baseSql.executeUpdate(updateBusinessDataHandleSign.replace("tableName", "das_data_pool_business").replace("idVal", businessData.getId().toString()));
                updateFinish(resultTableName, finance, type);
                log.info("replace success! resultTableName:{}, financeId:{}, businessId:{}", resultTableName, finance.getId(), businessData.getId());
                break;
            }
        }
        return "success";
    }

    private DataPool generDataPool(ReplaceBusiness finance) {
        BigDecimal premium;
        if (finance.getInsuranceTypeId().equals(1L)) {
            premium = finance.getSumFee().abs().divide(JQ_MIN_FEE_RATE, 2, RoundingMode.HALF_UP);
        } else {
            premium = finance.getSumFee().abs().divide(SY_PREMIUM_RATE[RandomUtils.nextInt(0, 3)], 2, RoundingMode.HALF_UP);
        }

        DataPool businessData = new DataPool();
        businessData.setPolicyNo(finance.getPolicyNo());
        businessData.setInsuranceTypeId(finance.getInsuranceTypeId());
        businessData.setInsuranceCompany(finance.getInsuranceCompany());
        businessData.setInsuranceCompanyId(finance.getInsuranceCompanyId());
        businessData.setProvinceId(finance.getProvinceId());
        businessData.setPremium(premium);
        businessData.setApplicant(finance.getApplicant());
        businessData.setOrderDate(finance.getFinanceOrderDate());
        return businessData;
    }

    private void insertBusRefLists(String resultTableName, ReplaceBusiness finance, DataPool businessData) throws SQLException {
        List<Map<String, Object>> insertMapList = new ArrayList<>();
        if (StringUtils.isNotEmpty(finance.getSids())) {
            for (String sId : finance.getSids().split(",")) {
                Map<String, Object> setMap = new HashMap<>();
                setMap.put("tableName", generateSettlementTableName(resultTableName));
                setMap.put("resultTableName", resultTableName);
                setMap.put("resultId", finance.getId());
                setMap.put("financeId", sId);
                setMap.put("businessData", businessData);
                insertMapList.add(setMap);
            }
        }
        if (StringUtils.isNotEmpty(finance.getCids())) {
            for (String cId : finance.getCids().split(",")) {
                Map<String, Object> setMap = new HashMap<>();
                setMap.put("tableName", generateCommissionTableName(resultTableName));
                setMap.put("resultTableName", resultTableName);
                setMap.put("resultId", finance.getId());
                setMap.put("financeId", cId);
                setMap.put("businessData", businessData);
                insertMapList.add(setMap);
            }
        }
        if (CollectionUtils.isNotEmpty(insertMapList)) {
            baseSql.executeInsert(insertBusinessRefList(insertMapList));
        }
    }

    private void updateFinish(String resultTableName, ReplaceBusiness finance, int type) throws SQLException {
        if (resultTableName.startsWith("result_")) {
            baseSql.executeUpdate(updateFinishHandleSign.replace("tableName", resultTableName).replace("idList", finance.getId().toString()));
        } else {
            if (type == 1) {
                baseSql.executeUpdate(updateFinishHandleSign.replace("tableName", resultTableName).replace("idList", finance.getId().toString()));
            } else {
                baseSql.executeUpdate(updateFinishHandleSign.replace("tableName", resultTableName).replace("idList", finance.getIds()));
            }
        }
    }

    private void insertBusinessRef(String resultTableName, ReplaceBusiness finance, DataPool businessData, int type) throws SQLException {
        List<Map<String, Object>> insertMapList = new ArrayList<>();
        if (resultTableName.startsWith("result_")) {
            insertBusRefLists(resultTableName, finance, businessData);
        } else {
            if (type == 1) {
                Map<String, Object> setMap = new HashMap<>();
                setMap.put("tableName", resultTableName);
                setMap.put("financeId", finance.getId());
                setMap.put("businessData", businessData);
                insertMapList.add(setMap);
                baseSql.executeInsert(insertBusinessRefList(insertMapList));
            } else {
                for (String id : finance.getIds().split(",")) {
                    Map<String, Object> setMap = new HashMap<>();
                    setMap.put("tableName", resultTableName);
                    setMap.put("financeId", id);
                    setMap.put("businessData", businessData);
                    insertMapList.add(setMap);
                }
                baseSql.executeInsert(insertBusinessRefList(insertMapList));
            }

        }
    }

    private String insertBusinessRefList(List<Map<String, Object>> insertMapList) {
        StringBuffer sb = new StringBuffer("insert into business_replace_ref (`table_name`, result_table_name, result_id, finance_id, business_id, policy_no, insurance_type_id, insurance_company, insurance_company_id, province_id, premium, applicant, order_date) values ");
        for (int i = 0; i < insertMapList.size(); i++) {
            Map<String, Object> map = insertMapList.get(i);
            DataPool businessData = (DataPool) map.get("businessData");
            sb.append("(").append(getMapStr(map, "tableName")).append(",");
            sb.append(getMapStr(map, "resultTableName")).append(",");
            sb.append(getMapStr(map, "resultId")).append(",");
            sb.append(getMapStr(map, "financeId")).append(",");
            sb.append(getSqlFormat(businessData.getId())).append(",");
            sb.append(getSqlFormat(businessData.getPolicyNo())).append(",");
            sb.append(getSqlFormat(businessData.getInsuranceTypeId())).append(",");
            sb.append(getSqlFormat(businessData.getInsuranceCompany())).append(",");
            sb.append(getSqlFormat(businessData.getInsuranceCompanyId())).append(",");
            sb.append(getSqlFormat(businessData.getProvinceId())).append(",");
            sb.append(getSqlFormat(businessData.getPremium())).append(",");
            sb.append(getSqlFormat(businessData.getApplicant())).append(",");
            sb.append(getSqlFormat(formatter.format(businessData.getOrderDate()))).append(")");
            if (i != insertMapList.size() - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    private String getMapStr(Map<String, Object> map, String key) {
        if (map.get(key) == null || StringUtils.isEmpty(map.get(key).toString())) {
            return null;
        } else {
            return "'" + map.get(key).toString() + "'";
        }
    }

    private String getSqlFormat(Object obj){
        if (obj == null || StringUtils.isEmpty(obj.toString())) {
            return null;
        }
        return "'" + obj.toString() + "'";
    }

    private static String generateSettlementTableName(String tableName) {
        String settlementTableName = "settlement_" + tableName.substring(7);
        return settlementTableName;
    }

    private String generateCommissionTableName(String tableName) {
        String commissionTableName = "commission_" + tableName.substring(7);
        return commissionTableName;
    }

    private DataPool transMap2Bean(GroovyRowResult groovyRowResult) {
        DataPool dataPool = new DataPool();
        try {
            BeanUtils.populate(dataPool, groovyRowResult);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return dataPool;
    }

    private LocalDate getBusinessStartDate(LocalDate financeOrderDate) {
        LocalDate startDate = LocalDate.of(2019, 1, 1);
        if (financeOrderDate == null) {
            return startDate;
        }
        LocalDate oneQuarterLimit = LocalDate.of(2019, 3, 31);
        if (financeOrderDate.isAfter(startDate) && financeOrderDate.isBefore(oneQuarterLimit)) {
            startDate = LocalDate.of(2018, 10, 1);
        }
        return startDate;
    }

    private String generInsuranceCompany(Long insuranceCompanyId, Long provinceId) {
        if (insuranceCompanyId.equals(RENBAO) && provinceId != null) {
            return "das_data_pool_business_" + insuranceCompanyId + "_" + provinceId;
        } else {
            return "das_data_pool_business_" + insuranceCompanyId;
        }
    }

    private List<ReplaceBusiness> replaceBusinessListByTableName(String resultTableName, int type) throws SQLException {
        List<GroovyRowResult> groovyRowResultList;
        if (resultTableName.startsWith("result_")) {
            groovyRowResultList = baseSql.rows(listReplaceBusiness.replace("resultTableNameVal", resultTableName));
        } else if(resultTableName.startsWith("settlement_")) {
            if (type == 1) {
                groovyRowResultList = baseSql.rows(listReplaceBusinessBySe.replace("resultTableNameVal", resultTableName));
            } else {
                groovyRowResultList = baseSql.rows(listReplaceBusinessBySeGroup.replace("resultTableNameVal", resultTableName));
            }
        } else {
            if (type == 1) {
                groovyRowResultList = baseSql.rows(listReplaceBusinessByCo.replace("resultTableNameVal", resultTableName));
            } else {
                groovyRowResultList = baseSql.rows(listReplaceBusinessByCoGroup.replace("resultTableNameVal", resultTableName));
            }
        }
        if (CollectionUtils.isEmpty(groovyRowResultList)) {
            return Collections.emptyList();
        }
        return transMapList2Bean(groovyRowResultList);
    }

    private List<ReplaceBusiness> transMapList2Bean(List<GroovyRowResult> groovyRowResultList) {
        List<ReplaceBusiness> replaceBusinesseList = new ArrayList<>();
        for (GroovyRowResult groovyRowResult : groovyRowResultList) {
            ReplaceBusiness replaceBusiness = new ReplaceBusiness();
            try {
                BeanUtils.populate(replaceBusiness, groovyRowResult);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            replaceBusinesseList.add(replaceBusiness);
        }
        return replaceBusinesseList;
    }

    public static LocalDate date2LocalDate(Date date) {
        if(null == date) {
            return null;
        }
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    }

    private List<String> generateThreeTableNames(String tableNameRef) {
        List tableNameList = new ArrayList();
        tableNameList.add("result_" + tableNameRef + "_2");
        tableNameList.add("commission_" + tableNameRef);
        tableNameList.add("settlement_" + tableNameRef);
        return tableNameList;
    }

}
