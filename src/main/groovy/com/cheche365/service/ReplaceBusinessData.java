package com.cheche365.service;

import com.cheche365.entity.DataPool;
import com.cheche365.entity.ReplaceBusiness;
import com.cheche365.util.ThreadPool;
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

    @Autowired
    private ReplaceHisBusiness replaceHisBusiness;

    @Autowired
    private ThreadPool runThreadPool;

    private static final BigDecimal JQ_MIN_FEE_RATE = new BigDecimal(0.04);
    private static final BigDecimal JQ_MAX_FEE_RATE = new BigDecimal(0.08);
    private static final BigDecimal SY_MIN_FEE_RATE = new BigDecimal(0.12);
    private static final BigDecimal SY_MAX_FEE_RATE = new BigDecimal(0.70);
    private static final BigDecimal[] SY_PREMIUM_RATE = new BigDecimal[]{new BigDecimal(0.1887), new BigDecimal(0.1698), new BigDecimal(0.1415)};
    //人保  财险
    private static final Long RENBAO = 2005L;

    private String allInsPro = "select concat(insurance_company_id, '_', province_id) as insPro\n" +
            "      from das_data_pool_business\n" +
            "      group by insurance_company_id, province_id";

    private String updateTableType = "update table_type set flag = 4 where type = 'typeVal'";
    private String updateEigntHandleSign = "update `tableNameVal` set handle_sign = 8 where id in (idListVal)";
    private String updateBusinessDataHandleSign = "update `tableName` set handle_sign = '2' where id = idVal";
    private String updateRepeatHandleSign = "update `tableName` set handle_sign = '3' where id = idVal";
    private String updateFinishHandleSign = "update `tableName` set handle_sign = 'handleSignVal' where id in (idList)";
    private String listReplaceBusiness = "select t1.id,\n" +
            "       t1.s_id                                                                                          as sids,\n" +
            "       t1.c_id                                                                                          as cids,\n" +
            "       保险公司                                                                                       as insuranceCompany,\n" +
            "       保险公司id                                                                                         as insuranceCompanyId,\n" +
            "       省                                                                                            as province,\n" +
            "       t3.id                                                                                         as provinceId,\n" +
            "       if (`8-险种名称` = '交强险', 1, 2)                                                              as insuranceTypeId,\n" +
            "       `9-保单出单日期`                                                                                as financeOrderDate,\n" +
            "       left(`9-保单出单日期`, 7)                                                                       as orderMonth, \n" +
            "       `6-保单单号` as policyNo, `5-投保人名称` as applicant, \n" +
            "        if((sum_fee + 0) >= (0 + sum_commission), (0+sum_fee), (0+sum_commission)) as sumFee\n" +
            "        from `resultTableNameVal` t1\n" +
            "       left join area t3\n" +
            "                 on t1.省 = t3.name\n" +
            "       left join (select * from business_replace_ref\n" +
            "         where result_table_name = 'resultTableNameVal'\n" +
            "         ) t4\n" +
            "                 on t1.id = t4.result_id\n" +
            "       where handle_sign = 3 \n" +
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

    private String updateThreeHandleSignFee = "update (select `6-保单单号`, `8-险种名称`, group_concat(id) as ids, sum(sum_commission) as sumFee\n" +
            "        from `settlementTableName`\n" +
            "        where handle_sign = 6\n" +
            "          and `8-险种名称` in ('交强险', '商业险')\n" +
            "          and left(`9-保单出单日期`, 4) = '2019'\n" +
            "          and sum_fee > 0\n" +
            "        group by `6-保单单号`, `8-险种名称`\n" +
            ") t1\n" +
            "  inner join `resultTableName` t2\n" +
            "  on t1.`6-保单单号` = t2.`6-保单单号`\n" +
            "    and t1.`8-险种名称` = t2.`8-险种名称`\n" +
            "set t2.handle_sign    = 3,\n" +
            "    t2.s_id           = if(t2.s_id is null, t1.ids,concat(t2.s_id, ',', t1.ids)),\n" +
            "    t2.sum_fee = (t2.sum_fee + t1.sumFee)";

    private String updateThreeHandleSignCom = "update (select `6-保单单号`, `8-险种名称`, group_concat(id) as ids, sum(sum_commission) as sumCommission\n" +
            "        from `commissionTableName`\n" +
            "        where handle_sign = 6\n" +
            "          and `8-险种名称` in ('交强险', '商业险')\n" +
            "          and left(`9-保单出单日期`, 4) = '2019'\n" +
            "          and sum_commission > 0\n" +
            "        group by `6-保单单号`, `8-险种名称`\n" +
            ") t1\n" +
            "  inner join `resultTableName` t2\n" +
            "  on t1.`6-保单单号` = t2.`6-保单单号`\n" +
            "    and t1.`8-险种名称` = t2.`8-险种名称`\n" +
            "set t2.handle_sign    = 3,\n" +
            "    t2.c_id           = if(t2.c_id is null, t1.ids,concat(t2.c_id, ',', t1.ids)),\n" +
            "    t2.sum_commission = (t2.sum_commission + t1.sumCommission)";

    private String updateFinishHandleSignFee = "update `settlementTableName` t1\n" +
            "set handle_sign = 10 \n" +
            "where t1.handle_sign = 6 and t1.`8-险种名称` in ('交强险', '商业险')\n" +
            "and left(t1.`9-保单出单日期`, 4) = '2019'\n" +
            "and t1.sum_fee > 0";

    private String updateFinishHandleSignCom = "update `commissionTableName` t1\n" +
            "set handle_sign = 10 \n" +
            "where t1.handle_sign = 6 and t1.`8-险种名称` in ('交强险', '商业险')\n" +
            "and left(t1.`9-保单出单日期`, 4) = '2019'\n" +
            "and t1.sum_commission > 0";

    private DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    public void replaceBusinessList(String tableNameRef) {
        try {
            //将散表中的收入或者成本相加为0的数据更新handle_sign=8
            updateScatterFlag(tableNameRef);
            List<GroovyRowResult> grsInsProList = baseSql.rows(allInsPro);
            if (CollectionUtils.isEmpty(grsInsProList)) {
                return;
            }
            List<String> insProList = grsInsProList.stream().map(it -> it.get("insPro").toString()).collect(Collectors.toList());
            String resultTableName = "result_" + tableNameRef + "_2";
            String settlementTableName = "settlement_" + tableNameRef;
            Map<String, Set<String>> financeInsProMap = getTargetInsProPolicyNo(resultTableName);
            replaceBusiness(settlementTableName, insProList, 2, financeInsProMap);
            replaceBusiness(settlementTableName, insProList, 1, financeInsProMap);
            String commissionTableName = "commission_" + tableNameRef;
            replaceBusiness(commissionTableName, insProList, 2, financeInsProMap);
            replaceBusiness(commissionTableName, insProList, 1, financeInsProMap);
            //将散表中的负数替换为history中的数据
            replaceHisBusiness.replaceHistoryBusiness(tableNameRef, financeInsProMap);
            //将散表中数据推到result中设置handlesign=3
            setResultThreeHs(commissionTableName, settlementTableName, resultTableName);
            //替换handle_sign为3的数据
            replaceBusiness(resultTableName, insProList, 1, financeInsProMap);
            //baseSql.executeUpdate(updateTableType.replace("typeVal", tableNameRef));
            log.info("replaceBusiness success! tableNameRef:{}", tableNameRef);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Map<String, Set<String>> getTargetInsProPolicyNo(String tableName) throws SQLException {
        List<GroovyRowResult> financeInsProNoResult = baseSql.rows("select `6-保单单号` as policyNo, 保险公司id as insuranceCompanyId from `" + tableName + "` where `8-险种名称` in ('交强险', '商业险')");
        if (financeInsProNoResult == null || financeInsProNoResult.size() == 0) {
            return null;
        }

        Map<String, Set<String>> financeInsProMap = financeInsProNoResult.stream()
                .collect(Collectors.groupingBy(it -> it.get("insuranceCompanyId").toString(), Collectors.mapping(it -> it.get("policyNo").toString(), Collectors.toSet())));
        return financeInsProMap;
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

    private String replaceBusiness(String resultTableName, List<String> allInsPro, int type, Map<String, Set<String>> financeInsProMap) throws SQLException, ExecutionException, InterruptedException {
        List<ReplaceBusiness> financeList = replaceBusinessListByTableName(resultTableName, type);

        runThreadPool.submitWithResult(financeList, finance -> {
            String insPro = finance.getInsuranceCompanyId() + "_" + finance.getProvinceId();
            if (!resultTableName.startsWith("result_") && !allInsPro.contains(insPro)) {
                return null;
            }
            updateReplaceData(finance, allInsPro, resultTableName, type, financeInsProMap);
            return null;
        });

        log.info("replaceBusiness success! resultTableName: {}, type:{}", resultTableName, type);
        return "success";
    }

    private String updateReplaceData(ReplaceBusiness finance, List<String> allInsPro, String resultTableName, int type, Map<String, Set<String>> financeInsProMap) throws SQLException {
        synchronized (("run_thread_" + finance.getInsuranceCompanyId() + "_" + finance.getProvinceId()).intern()) {
            BigDecimal minPremium, maxPremium;
            //保险公司id 是空
            String insPro = finance.getInsuranceCompanyId() + "_" + finance.getProvinceId();
            if (finance.getInsuranceCompanyId() == null || !allInsPro.contains(insPro)) {
                if (resultTableName.startsWith("result_")) {
                    forceReplaceBusiness(finance, resultTableName, type);
                }
                return null;
            }
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
                DataPool businessData = findBusinessData(finance, insuranceCompanyTableName, minPremium, maxPremium, financeInsProMap, resultTableName);
                boolean isFindBusiness = false;
                if (businessData == null) {
                    if (resultTableName.startsWith("result_")) {
                        businessData = generDataPool(finance);
                    } else {
                        break;
                    }
                } else {
                    String getReplaceBusinessId = "select business_id as businessId from business_replace_ref where business_id = " + businessData.getId() + " and business_table_name = '" + insuranceCompanyTableName + "' limit 1";
                    GroovyRowResult businessId = baseSql.firstRow(getReplaceBusinessId);
                    if (businessId != null && businessId.get("businessId") != null) {
                        log.error("has use repeat businessData! financeId:{}, businessId:{}", finance.getId(), businessData.getId());
                        continue;
                    }
                    isFindBusiness = true;
                }

                if (businessData == null) {
                    break;
                }
                if (finance.getSumFee().compareTo(BigDecimal.ZERO) < 0) {
                    businessData.setPremium(BigDecimal.ZERO.subtract(businessData.getPremium()));
                }
                insertBusinessRef(resultTableName, finance, businessData, type, isFindBusiness ? insuranceCompanyTableName : null);
                if (isFindBusiness) {
                    baseSql.executeUpdate(updateBusinessDataHandleSign.replace("tableName", insuranceCompanyTableName).replace("idVal", String.valueOf(businessData.getId())));
                    baseSql.executeUpdate(updateBusinessDataHandleSign.replace("tableName", "das_data_pool_business").replace("idVal", String.valueOf(businessData.getId())));
                }
                updateFinish(resultTableName, finance, type, isFindBusiness);
                log.info("replace success! resultTableName:{}, financeId:{}, businessId:{}", resultTableName, finance.getId(), businessData.getId());
                break;
            }
        }
        return "success";
    }

    /**
     * 查找对应的业务数据
     * @param finance
     * @param insuranceCompanyTableName
     * @param minPremium
     * @param maxPremium
     * @return
     * @throws SQLException
     */
    private DataPool findBusinessData(ReplaceBusiness finance, String insuranceCompanyTableName, BigDecimal minPremium, BigDecimal maxPremium, Map<String, Set<String>> financeInsProMap, String resultTableName) throws SQLException {
        GroovyRowResult groovyRowResult = findUniqueBusinessData(finance, insuranceCompanyTableName, minPremium, maxPremium);
        if (groovyRowResult == null || groovyRowResult.size() == 0) {
            return null;
        }
        if (financeInsProMap == null || financeInsProMap.size() == 0) {
            return transMap2Bean(groovyRowResult);
        }

        Set<String> policyNoSet = financeInsProMap.get(groovyRowResult.get("insuranceCompanyId").toString());
        if (CollectionUtils.isEmpty(policyNoSet)) {
            return transMap2Bean(groovyRowResult);
        }

        int num = 0;
        String policyNo = groovyRowResult.get("policyNo").toString();
        while (policyNoSet.contains(policyNo)) {
            if (num >= 100) {
                break;
            }

            //将重复数据更新掉
            baseSql.executeUpdate(updateRepeatHandleSign.replace("tableName", insuranceCompanyTableName).replace("idVal", String.valueOf(groovyRowResult.get("id"))));
            baseSql.executeUpdate(updateRepeatHandleSign.replace("tableName", "das_data_pool_business").replace("idVal", String.valueOf(groovyRowResult.get("id"))));
            groovyRowResult = findUniqueBusinessData(finance, insuranceCompanyTableName, minPremium, maxPremium);
            if (groovyRowResult == null || groovyRowResult.size() == 0) {
                break;
            }
            policyNo = groovyRowResult.get("policyNo").toString();
            num++;
        }
        return transMap2Bean(groovyRowResult);
    }

    private GroovyRowResult findUniqueBusinessData(ReplaceBusiness finance, String insuranceCompanyTableName, BigDecimal minPremium, BigDecimal maxPremium) throws SQLException {
        LocalDate startDate = getBusinessStartDate(date2LocalDate(finance.getFinanceOrderDate()));
        String startDateStr = startDate.format(fmt);
        String getReplaceData = "select id, policy_no as policyNo, insurance_type_id as insuranceTypeId, insurance_company as insuranceCompany, insurance_company_id as insuranceCompanyId, province_id as provinceId, premium, applicant, order_date as orderDate from " + insuranceCompanyTableName;
        getReplaceData += " where ";
        if (finance.getProvinceId() != null) {
            getReplaceData += " province_id = " + finance.getProvinceId() + " and ";
        }
        getReplaceData += " order_date >= '" + startDateStr + "' ";
        if (finance.getFinanceOrderDate() != null) {
            getReplaceData += " and order_date <= '" + formatter.format(finance.getFinanceOrderDate()) + "' ";
        }
        getReplaceData += " and premium >= '" + minPremium + "' and premium <= '" + maxPremium + "'";
        getReplaceData += " and handle_sign = 0 order by order_date desc, id desc limit 1";
        GroovyRowResult dataPoolList = baseSql.firstRow(getReplaceData);
        if (dataPoolList == null || dataPoolList.size() == 0) {
            return null;
        }
        return (GroovyRowResult) dataPoolList.get(0);
    }

    private void forceReplaceBusiness(ReplaceBusiness finance, String resultTableName, int type) throws SQLException {
        DataPool businessData = generDataPool(finance);
        insertBusinessRef(resultTableName, finance, businessData, type, null);
        updateFinish(resultTableName, finance, type, false);
        log.info("replace success! resultTableName:{}, financeId:{}, businessId:{}", resultTableName, finance.getId(), businessData.getId());
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

    private void insertBusRefLists(String resultTableName, ReplaceBusiness finance, DataPool businessData, String insuranceCompanyTableName) throws SQLException {
        List<Map<String, Object>> insertMapList = new ArrayList<>();
        if (StringUtils.isNotEmpty(finance.getSids())) {
            for (String sId : finance.getSids().split(",")) {
                Map<String, Object> setMap = new HashMap<>();
                setMap.put("tableName", generateSettlementTableName(resultTableName));
                setMap.put("resultTableName", resultTableName);
                setMap.put("resultId", finance.getId());
                setMap.put("businessTableName", insuranceCompanyTableName);
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
                setMap.put("businessTableName", insuranceCompanyTableName);
                setMap.put("financeId", cId);
                setMap.put("businessData", businessData);
                insertMapList.add(setMap);
            }
        }
        if (CollectionUtils.isNotEmpty(insertMapList)) {
            baseSql.executeInsert(insertBusinessRefList(insertMapList));
        }
    }

    private void updateFinish(String resultTableName, ReplaceBusiness finance, int type, boolean isFindBusiness) throws SQLException {
        int handleSignFinish = isFindBusiness ? 9 : 10;
        if (resultTableName.startsWith("result_")) {
            baseSql.executeUpdate(updateFinishHandleSign.replace("tableName", resultTableName).replace("idList", finance.getId().toString()).replace("handleSignVal", String.valueOf(handleSignFinish)));
        } else {
            if (type == 1) {
                baseSql.executeUpdate(updateFinishHandleSign.replace("tableName", resultTableName).replace("idList", finance.getId().toString()).replace("handleSignVal", String.valueOf(handleSignFinish)));
            } else {
                baseSql.executeUpdate(updateFinishHandleSign.replace("tableName", resultTableName).replace("idList", finance.getIds()).replace("handleSignVal", String.valueOf(handleSignFinish)));
            }
        }
    }

    public void insertBusinessRef(String resultTableName, ReplaceBusiness finance, DataPool businessData, int type, String insuranceCompanyTableName) throws SQLException {
        List<Map<String, Object>> insertMapList = new ArrayList<>();
        if (resultTableName.startsWith("result_")) {
            insertBusRefLists(resultTableName, finance, businessData, insuranceCompanyTableName);
        } else {
            if (type == 1) {
                Map<String, Object> setMap = new HashMap<>();
                setMap.put("tableName", resultTableName);
                setMap.put("financeId", finance.getId());
                setMap.put("businessTableName", insuranceCompanyTableName);
                setMap.put("businessData", businessData);
                insertMapList.add(setMap);
                baseSql.executeInsert(insertBusinessRefList(insertMapList));
            } else {
                for (String id : finance.getIds().split(",")) {
                    Map<String, Object> setMap = new HashMap<>();
                    setMap.put("tableName", resultTableName);
                    setMap.put("financeId", id);
                    setMap.put("businessTableName", insuranceCompanyTableName);
                    setMap.put("businessData", businessData);
                    insertMapList.add(setMap);
                }
                baseSql.executeInsert(insertBusinessRefList(insertMapList));
            }

        }
    }

    public static String insertBusinessRefList(List<Map<String, Object>> insertMapList) {
        StringBuffer sb = new StringBuffer("insert into business_replace_ref (`table_name`, result_table_name, business_table_name, result_id, finance_id, business_id, policy_no, insurance_type, insurance_type_id, insurance_company, insurance_company_id, province_id, premium, applicant, order_date) values ");
        for (int i = 0; i < insertMapList.size(); i++) {
            Map<String, Object> map = insertMapList.get(i);
            DataPool businessData = (DataPool) map.get("businessData");
            sb.append("(").append(getMapStr(map, "tableName")).append(",");
            sb.append(getMapStr(map, "resultTableName")).append(",");
            sb.append(getMapStr(map, "businessTableName")).append(",");
            sb.append(getMapStr(map, "resultId")).append(",");
            sb.append(getMapStr(map, "financeId")).append(",");
            sb.append(getSqlFormat(businessData.getId())).append(",");
            sb.append(getSqlFormat(businessData.getPolicyNo())).append(",");
            sb.append(getSqlFormat(businessData.getInsuranceType())).append(",");
            sb.append(getSqlFormat(businessData.getInsuranceTypeId())).append(",");
            sb.append(getSqlFormat(businessData.getInsuranceCompany())).append(",");
            sb.append(getSqlFormat(businessData.getInsuranceCompanyId())).append(",");
            sb.append(getSqlFormat(businessData.getProvinceId())).append(",");
            sb.append(getSqlFormat(businessData.getPremium())).append(",");
            sb.append(getSqlFormat(businessData.getApplicant())).append(",");
            if (businessData.getOrderDate() != null) {
                sb.append(getSqlFormat(formatter.format(businessData.getOrderDate()))).append(")");
            } else {
                sb.append(getSqlFormat(null)).append(")");
            }
            if (i != insertMapList.size() - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    private static String getMapStr(Map<String, Object> map, String key) {
        if (map.get(key) == null || StringUtils.isEmpty(map.get(key).toString())) {
            return null;
        } else {
            return "'" + map.get(key).toString() + "'";
        }
    }

    private static String getSqlFormat(Object obj) {
        if (obj == null || StringUtils.isEmpty(obj.toString())) {
            return null;
        }
        return "'" + obj.toString() + "'";
    }

    public static String getSqlFormatList(List<String> objList) {
        StringBuffer sb = new StringBuffer();
        if (CollectionUtils.isEmpty(objList)) {
            return null;
        }

        for (int i = 0; i < objList.size(); i++) {
            sb.append("'").append(objList.get(i)).append("'");
            if (i != objList.size() - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    private String generateSettlementTableName(String tableName) {
        String settlementTableName = "settlement_" + tableName.substring(7, tableName.lastIndexOf("_"));
        return settlementTableName;
    }

    private String generateCommissionTableName(String tableName) {
        String commissionTableName = "commission_" + tableName.substring(7, tableName.lastIndexOf("_"));
        return commissionTableName;
    }

    public static DataPool transMap2Bean(GroovyRowResult groovyRowResult) {
        if (groovyRowResult == null || groovyRowResult.size() == 0) {
            return null;
        }
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
            groovyRowResultList = baseSql.rows(listReplaceBusiness.replaceAll("resultTableNameVal", resultTableName));
        } else if (resultTableName.startsWith("settlement_")) {
            if (type == 1) {
                groovyRowResultList = baseSql.rows(listReplaceBusinessBySe.replaceAll("resultTableNameVal", resultTableName));
            } else {
                groovyRowResultList = baseSql.rows(listReplaceBusinessBySeGroup.replaceAll("resultTableNameVal", resultTableName));
            }
        } else {
            if (type == 1) {
                groovyRowResultList = baseSql.rows(listReplaceBusinessByCo.replaceAll("resultTableNameVal", resultTableName));
            } else {
                groovyRowResultList = baseSql.rows(listReplaceBusinessByCoGroup.replaceAll("resultTableNameVal", resultTableName));
            }
        }
        if (CollectionUtils.isEmpty(groovyRowResultList)) {
            return Collections.emptyList();
        }
        return transMapList2Bean(groovyRowResultList);
    }

    public static List<ReplaceBusiness> transMapList2Bean(List<GroovyRowResult> groovyRowResultList) {
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
        if (null == date) {
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
