package com.cheche365.service;

import com.cheche365.entity.TwoHandleSign;
import com.cheche365.util.CommonUtils;
import com.cheche365.util.ThreadPool;
import com.cheche365.util.Utils;
import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * author:WangZhaoliang
 * Date:2020/4/12 17:46
 */
@Log4j2
@Service
public class MatchSingleData {

    @Autowired
    private Sql baseSql;
    @Autowired
    private ThreadPool runThreadPool;

    private static final BigDecimal MAX_GROSS_MARGIN = BigDecimal.ONE;
    private static final BigDecimal MIN_GROSS_MARGIN = new BigDecimal(-1);

    private String listResultTwoHandleSign = "      select t1.id,\n" +
            "             s_id as sids,\n" +
            "             c_id as cids,\n" +
            "             `8-险种名称` as insuranceType,\n" +
            "             保险公司id as insuranceCompanyId,\n" +
            "             t1.省 as province,\n" +
            "             `40-代理人名称` as agentName,\n" +
            "             left(`9-保单出单日期`, 7)                   as orderMonth,\n" +
            "             ifnull(`sum_fee`, 0.00) as fee,\n" +
            "             ifnull(`sum_commission`, 0.00) as commission, \n" +
            "             ifnull(`gross_profit`, 0.00) as grossMargin, \n" +
            "             r_flag as rFlag\n" +
            "      from `resultTableName` t1\n" +
            "      where handle_sign = 2\n" +
            "      and `8-险种名称` in ('交强险', '商业险') " +
            "      and date_format(`9-保单出单日期`,'%Y')='2019'";

    private String listSingleFeeCommissionData = "select t1.id,\n" +
            "        s_id as sids,\n" +
            "        c_id as cids,\n" +
            "        `8-险种名称` as insuranceType,\n" +
            "        保险公司id as insuranceCompanyId,\n" +
            "        `40-代理人名称` as agentName,\n" +
            "        `省` as province,\n" +
            "        left(`9-保单出单日期`, 7) as orderMonth,\n" +
            "        ifnull(`sum_fee`, 0.00) as fee,\n" +
            "        ifnull(`sum_commission`, 0.00) as commission,\n" +
            "        r_flag as rFlag\n" +
            "        from `resultTableName` t1\n" +
            "        where handle_sign = 6\n" +
            "        and `8-险种名称` in ('交强险', '商业险')" +
            "        and date_format(`9-保单出单日期`,'%Y')='2019'";

    private String updateHandleSignList = "update `resultTableName` set handle_sign = handleSignVal where id in (idListVal)";
    private String updateFourHandleSignList = "update `resultTableName` set handle_sign = handleSignVal, sum_fee = 'realFeeVal', sum_commission = 'realCommissionVal', gross_profit = 'grossProfitVal',c_id='cIds',s_id='sIds' where id in (idListVal)";

    private String insertResultRefList = "insert into result_gross_margin_ref (`table_name`, result_id, s_id, c_id, `type`, real_fee, real_commission) values ";
    private DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");


    public void matchSingleDataList(String tableNameRef, boolean isResultName) {
        try {
            String resultTableName = "result_" + tableNameRef + "_2";
            String settlementTableName = "settlement_" + tableNameRef;
            String commissionTableName = "commission_" + tableNameRef;

            List<String> targetTableName = new ArrayList<>();
            if (isResultName) {
                targetTableName.add(resultTableName);
            } else {
                targetTableName.add(settlementTableName);
                targetTableName.add(commissionTableName);
            }

            for (String tTableName : targetTableName) {
                List<GroovyRowResult> groovyRowResultList = new ArrayList<>();
                if (tTableName.startsWith("result_")) {
                    groovyRowResultList = baseSql.rows(listResultTwoHandleSign.replace("resultTableName", resultTableName));
                } else if (tTableName.startsWith("settlement_")) {
                    groovyRowResultList = baseSql.rows((listResultTwoHandleSign + " and (ifnull(`gross_profit`, 0.00)) < -1 ").replace("resultTableName", resultTableName));
                } else {
                    groovyRowResultList = baseSql.rows((listResultTwoHandleSign + " and (ifnull(`gross_profit`, 0.00)) > 1 ").replace("resultTableName", resultTableName));
                }
                if (CollectionUtils.isEmpty(groovyRowResultList)) {
                    return;
                }
                List<TwoHandleSign> twoHandleSignList = transMapToBean(groovyRowResultList);

                List<GroovyRowResult> grsSourceDataList = baseSql.rows(listSingleFeeCommissionData.replace("resultTableName", tTableName));
                if (CollectionUtils.isEmpty(grsSourceDataList)) {
                    return;
                }

                List<TwoHandleSign> allSourceDataList = transMapToBean(grsSourceDataList);
                Map<String, List<TwoHandleSign>> thsMapList = allSourceDataList.stream()
                        .collect(Collectors.groupingBy(it -> generRateMapKey(it)));

                runThreadPool.submitWithResult(twoHandleSignList, twoHandleSign -> {
                    if (thsMapList.containsKey(generRateMapKey(twoHandleSign))) {
                        matchSingleDta(thsMapList, twoHandleSign, resultTableName, tTableName);
                    }
                    return null;
                });

                log.info("matchSingleDataList success! tTableName:{}", tTableName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<TwoHandleSign> transMapToBean(List<GroovyRowResult> groovyRowResultList) {
        List<TwoHandleSign> thsList = new ArrayList<>();
        for (GroovyRowResult groovyRowResult : groovyRowResultList) {
            TwoHandleSign twoHandleSign = new TwoHandleSign();
            try {
                BeanUtils.populate(twoHandleSign, groovyRowResult);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            thsList.add(twoHandleSign);
        }
        return thsList;
    }

    private String matchSingleDta(Map<String, List<TwoHandleSign>> thsMapLIst, TwoHandleSign twoHandleSign, String resultTableName, String tableName) throws SQLException {
        if (twoHandleSign.getGrossMargin().compareTo(MIN_GROSS_MARGIN) < 0) {
            twoHandleSignAddFinance(thsMapLIst, twoHandleSign, resultTableName, tableName, 1);
        }

        if (twoHandleSign.getGrossMargin().compareTo(MAX_GROSS_MARGIN) > 0) {
            twoHandleSignAddFinance(thsMapLIst, twoHandleSign, resultTableName, tableName, 2);
        }
        return "success";
    }

    private void twoHandleSignAddFinance(Map<String, List<TwoHandleSign>> thsListMap, TwoHandleSign twoHandleSign, String resultTableName, String tableName, int type) throws SQLException {
        String lock;
        if (type == 1) {
            lock = ("TWO_FEE_THREAD_" + generRateMapKey(twoHandleSign) + "_END").intern();
        } else {
            lock = ("TWO_COMMISSION_THREAD_" + generRateMapKey(twoHandleSign) + "_END").intern();
        }

        if (StringUtils.isEmpty(twoHandleSign.getOrderMonth())) {
            twoHandleSign.setOrderMonth("2019-01");
        }
        synchronized (lock) {
            List<TwoHandleSign> insProOrderMonthList = thsListMap.get(generRateMapKey(twoHandleSign));
            List<TwoHandleSign> allSourceDataList = new ArrayList<>();
            LocalDate targetOrderMonth = LocalDate.parse(twoHandleSign.getOrderMonth() + "-01", fmt);
            if (type == 1) {
                allSourceDataList = insProOrderMonthList.stream()
                        .filter(it -> StringUtils.isNotEmpty(it.getSids()) && StringUtils.isEmpty(it.getCids()))
                        .filter(it -> StringUtils.isNotEmpty(it.getOrderMonth()))
                        .filter(it -> (it.getOrderMonth().equals(twoHandleSign.getOrderMonth())
                                || LocalDate.parse(it.getOrderMonth() + "-01", fmt).isAfter(targetOrderMonth)))
                        .collect(Collectors.toList());
            } else {
                if (StringUtils.isEmpty(twoHandleSign.getAgentName())) {
                    allSourceDataList = insProOrderMonthList.stream()
                            .filter(it -> StringUtils.isNotEmpty(it.getCids()) && StringUtils.isEmpty(it.getSids()))
                            .filter(it -> StringUtils.isEmpty(it.getAgentName()))
                            .filter(it -> StringUtils.isNotEmpty(it.getOrderMonth()))
                            .filter(it -> (it.getOrderMonth().equals(twoHandleSign.getOrderMonth())
                                    || LocalDate.parse(it.getOrderMonth() + "-01", fmt).isAfter(targetOrderMonth)))
                            .collect(Collectors.toList());
                } else {
                    allSourceDataList = insProOrderMonthList.stream()
                            .filter(it -> StringUtils.isNotEmpty(it.getCids()) && StringUtils.isEmpty(it.getSids()))
                            .filter(it -> it.getAgentName().equals(twoHandleSign.getAgentName()))
                            .filter(it -> StringUtils.isNotEmpty(it.getOrderMonth()))
                            .filter(it -> (it.getOrderMonth().equals(twoHandleSign.getOrderMonth())
                                    || LocalDate.parse(it.getOrderMonth() + "-01", fmt).isAfter(targetOrderMonth)))
                            .collect(Collectors.toList());
                }
            }
            if (CollectionUtils.isEmpty(allSourceDataList)) {
                return;
            }

            BigDecimal sumFee = twoHandleSign.getFee();
            BigDecimal sumCommission = twoHandleSign.getCommission();
            boolean matchSuccess = false;
            List<TwoHandleSign> resultThsList = new ArrayList<>();
            List<Long> filterList = new ArrayList<>();
            int times = 1;

            while (true) {
                if (times > 10) {
                    break;
                }
                List<TwoHandleSign> sourceDataList = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(filterList)) {
                    sourceDataList = allSourceDataList.stream()
                            .filter(it -> !filterList.contains(it.getId()))
                            .limit(5)
                            .collect(Collectors.toList());
                } else {
                    sourceDataList = allSourceDataList.stream().limit(5).collect(Collectors.toList());
                }
                if (CollectionUtils.isEmpty(sourceDataList)) {
                    break;
                }

                BigDecimal sourceTotalVal = BigDecimal.ZERO;
                if (type == 1) {
                    sourceTotalVal = sourceDataList.stream().map(TwoHandleSign::getFee).reduce(BigDecimal.ZERO, BigDecimal::add);
                    sumFee = sumFee.add(sourceTotalVal);
                } else {
                    sourceTotalVal = sourceDataList.stream().map(TwoHandleSign::getCommission).reduce(BigDecimal.ZERO, BigDecimal::add);
                    sumCommission = sumCommission.add(sourceTotalVal);
                }
                BigDecimal grossMargin = getFinanceGrossMargin(sumFee, sumCommission);
                if (grossMargin.abs().compareTo(BigDecimal.ONE) <= 0 && sumFee.compareTo(BigDecimal.ZERO) > 0) {
                    resultThsList.addAll(sourceDataList);
                    matchSuccess = true;
                    break;
                } else {
                    if (type == 1) {
                        sumFee = sumFee.subtract(sourceTotalVal);
                        List<TwoHandleSign> nearGrossMargin = matchNearGrossMargin(sumFee, sumCommission, sourceDataList, type);
                        if (CollectionUtils.isNotEmpty(nearGrossMargin)) {
                            sumFee = sumFee.add(nearGrossMargin.stream().map(TwoHandleSign::getFee).reduce(BigDecimal.ZERO, BigDecimal::add));
                            resultThsList.addAll(nearGrossMargin);
                            matchSuccess = true;
                            break;
                        } else {
                            filterList.addAll(sourceDataList.stream().map(TwoHandleSign::getId).collect(Collectors.toList()));
                            times++;
                        }
                    } else {
                        sumCommission = sumCommission.subtract(sourceTotalVal);
                        List<TwoHandleSign> nearGrossMargin = matchNearGrossMargin(sumFee, sumCommission, sourceDataList, type);
                        if (CollectionUtils.isNotEmpty(nearGrossMargin)) {
                            sumCommission = sumCommission.add(nearGrossMargin.stream().map(TwoHandleSign::getCommission).reduce(BigDecimal.ZERO, BigDecimal::add));
                            resultThsList.addAll(nearGrossMargin);
                            matchSuccess = true;
                            break;
                        } else {
                            filterList.addAll(sourceDataList.stream().map(TwoHandleSign::getId).collect(Collectors.toList()));
                            times++;
                        }
                    }
                }
            }

            if (matchSuccess) {
                insertResultRef(resultTableName, resultThsList, twoHandleSign, sumFee, sumCommission, type);
                String idStrList = StringUtils.join(resultThsList.stream().map(it -> it.getId().toString()).collect(Collectors.toList()).toArray(), ",");

                StringJoiner sIds = new StringJoiner(",");
                StringJoiner cIds = new StringJoiner(",");
                if (twoHandleSign.getSids() != null) {
                    sIds.add(twoHandleSign.getSids());
                }

                if (twoHandleSign.getCids() != null) {
                    cIds.add(twoHandleSign.getCids());
                }

                if (type == 1) {
                    for (TwoHandleSign handleSign : resultThsList) {
                        sIds.add(handleSign.getSids());
                    }
                } else {
                    for (TwoHandleSign handleSign : resultThsList) {
                        cIds.add(handleSign.getCids());
                    }
                }

                baseSql.executeUpdate(updateHandleSignList.replace("resultTableName", tableName)
                        .replace("idListVal", idStrList)
                        .replace("handleSignVal", "5"));
                baseSql.executeUpdate(updateFourHandleSignList.replace("resultTableName", resultTableName)
                        .replace("idListVal", twoHandleSign.getId() + "")
                        .replace("handleSignVal", "4")
                        .replace("realFeeVal", sumFee.toString())
                        .replace("realCommissionVal", sumCommission.toString())
                        .replace("grossProfitVal", getFinanceGrossMargin(sumFee, sumCommission).toString())
                        .replace("sIds", sIds.toString())
                        .replace("cIds", cIds.toString())
                );
                thsListMap.put(generRateMapKey(twoHandleSign), getRemoveList(insProOrderMonthList, resultThsList));
                log.info("matchSingleData success! twoHandleSignId:{}", twoHandleSign.getId());
            } else {
                log.info("matchSingleData failed! twoHandleSignId:{}", twoHandleSign.getId());
            }
        }
    }

    private List<TwoHandleSign> matchNearGrossMargin(BigDecimal sumFee, BigDecimal sumCommission, List<TwoHandleSign> sourceDataList, int type) {
        List<TwoHandleSign> thsList = new ArrayList<>();
        List resultIdList = sourceDataList.stream().map(it -> it.getId().toString()).collect(Collectors.toList());
        Map<String, List<TwoHandleSign>> twoHandleSignMap = sourceDataList.stream().collect(Collectors.groupingBy(it -> it.getId().toString()));
        boolean continueLoop = true;
        for (int i = sourceDataList.size() - 1; i > 0; i--) {
            List<List<String>> sIdList = Utils.combine(resultIdList, i);
            for (List<String> rIds : sIdList) {
                List<TwoHandleSign> thsSumList = getThsByRids(rIds, twoHandleSignMap);
                if (type == 1) {
                    sumFee = sumFee.add(thsSumList.stream().map(TwoHandleSign::getFee).reduce(BigDecimal.ZERO, BigDecimal::add));
                } else {
                    sumCommission = sumCommission.add(thsSumList.stream().map(TwoHandleSign::getCommission).reduce(BigDecimal.ZERO, BigDecimal::add));
                }
                if (isNormalGrossMargin(sumFee, sumCommission)) {
                    thsList = thsSumList;
                    continueLoop = false;
                    break;
                }
            }
            if (!continueLoop) {
                break;
            }
        }
        return thsList;
    }

    private List<TwoHandleSign> getRemoveList(List<TwoHandleSign> insProOrderMonthList, List<TwoHandleSign> resultThsList) {
        List<Long> resultIdList = resultThsList.stream().map(TwoHandleSign::getId).collect(Collectors.toList());
        List<TwoHandleSign> thsList = insProOrderMonthList.stream().filter(it -> !resultIdList.contains(it.getId())).collect(Collectors.toList());
        return thsList;
    }

    private boolean isNormalGrossMargin(BigDecimal sumFee, BigDecimal sumCommission) {
        return getFinanceGrossMargin(sumFee, sumCommission).abs().compareTo(BigDecimal.ONE) <= 0 && sumFee.compareTo(BigDecimal.ZERO) > 0;
    }

    private List<TwoHandleSign> getThsByRids(List<String> rIds, Map<String, List<TwoHandleSign>> twoHandleSignMap) {
        List<TwoHandleSign> twoHandleSignList = new ArrayList<>();
        for (String rId : rIds) {
            twoHandleSignList.add(twoHandleSignMap.get(rId).get(0));
        }
        return twoHandleSignList;
    }

    private void insertResultRef(String resultName, List<TwoHandleSign> resultThsList, TwoHandleSign twoHandleSign, BigDecimal sumFee, BigDecimal sumCommission, int type) throws SQLException {
        List<Map<String, Object>> insertMapList = new ArrayList<>();
        if (type == 1) {
            String cId;
            if (twoHandleSign.getRFlag() == 1) {
                cId = twoHandleSign.getSids().split(",")[0];
                type = 3;
            } else {
                cId = twoHandleSign.getCids().split(",")[0];
                type = 2;
            }
            for (TwoHandleSign ths : resultThsList) {
                for (String sId : ths.getSids().split(",")) {
                    Map<String, Object> insertMap = makeInsertMap(resultName, twoHandleSign, sumFee, sumCommission, sId, cId, type);
                    insertMapList.add(insertMap);
                }
            }
        } else {
            String sId;
            if (twoHandleSign.getRFlag() == 2) {
                sId = twoHandleSign.getCids().split(",")[0];
                type = 4;
            } else {
                sId = twoHandleSign.getSids().split(",")[0];
                type = 1;
            }
            for (TwoHandleSign ths : resultThsList) {
                for (String cId : ths.getCids().split(",")) {
                    Map<String, Object> insertMap = makeInsertMap(resultName, twoHandleSign, sumFee, sumCommission, sId, cId, type);
                    insertMapList.add(insertMap);
                }
            }
        }
        baseSql.executeInsert(makeMapToStr(insertMapList));
    }

    private String makeMapToStr(List<Map<String, Object>> insertMapList) {
        StringBuffer sb = new StringBuffer(insertResultRefList);
        for (int i = 0; i < insertMapList.size(); i++) {
            Map<String, Object> map = insertMapList.get(i);
            sb.append(" (");
            sb.append(CommonUtils.getMapStr(map, "tableName")).append(",");
            sb.append(CommonUtils.getMapStr(map, "resultId")).append(",");
            sb.append(CommonUtils.getMapStr(map, "sId")).append(",");
            sb.append(CommonUtils.getMapStr(map, "cId")).append(",");
            sb.append(CommonUtils.getMapStr(map, "type")).append(",");
            sb.append(CommonUtils.getMapStr(map, "realFee")).append(",");
            sb.append(CommonUtils.getMapStr(map, "realCommission")).append(")");
            if (i != insertMapList.size() - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    private Map<String, Object> makeInsertMap(String resultName, TwoHandleSign twoHandleSign, BigDecimal sumFee, BigDecimal sumCommission, String sId, String cId, int type) {
        Map<String, Object> insertMap = new HashMap<>();
        insertMap.put("tableName", resultName);
        insertMap.put("resultId", twoHandleSign.getId());
        insertMap.put("cId", cId);
        insertMap.put("sId", sId);
        insertMap.put("type", type);
        insertMap.put("realFee", sumFee);
        insertMap.put("realCommission", sumCommission);
        return insertMap;
    }

    /**
     * 根据收入成本获取毛利率
     *
     * @param sumFee
     * @param sumCommission
     * @return
     */
    private BigDecimal getFinanceGrossMargin(BigDecimal sumFee, BigDecimal sumCommission) {
        return (sumFee.subtract(sumCommission)).divide(sumFee, 6, RoundingMode.HALF_UP);
    }

    private String generRateMapKey(TwoHandleSign twoHandleSign) {
        return twoHandleSign.getInsuranceCompanyId() + "_" + twoHandleSign.getProvince() + "_" + twoHandleSign.getOrderMonth();
    }

}
