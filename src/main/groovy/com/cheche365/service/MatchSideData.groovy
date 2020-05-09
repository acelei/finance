package com.cheche365.service

import com.cheche365.util.MatchResult
import com.cheche365.util.ThreadPool
import com.cheche365.util.Utils
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
@Slf4j
class MatchSideData {
    @Autowired
    Sql baseSql
    @Autowired
    private ThreadPool runThreadPool

    String querySettlement = "select id,id as s_id,sum_fee as fee,sum_commission as commission,`14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,`42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,DATE_FORMAT(`9-保单出单日期`,'%Y-%m') as order_month,`保险公司`,`省`, null as flag,0 as r_flag from settlement_# where `8-险种名称` in ('交强险','商业险') and handle_sign=6 and date_format(`9-保单出单日期`,'%Y')='2019'"
    String queryCommission = "select id,id as c_id,sum_fee as fee,sum_commission as commission,`14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,`42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,DATE_FORMAT(`9-保单出单日期`,'%Y-%m') as order_month,`保险公司`,`省`,`40-代理人名称`, null as flag,0 as r_flag from commission_# where `8-险种名称` in ('交强险','商业险') and handle_sign=6 and date_format(`9-保单出单日期`,'%Y')='2019'"

    void run(String type) {
        log.info("匹配单边数据-结算(付佣使用结算业务数据):{}", type)
        processSettlement(type)
        log.info("匹配单边数据-结算(付佣使用结算业务数据)完成:{}", type)
        log.info("匹配单边数据-佣金(结算使用付佣业务数据):{}", type)
        processCommission(type)
        log.info("匹配单边数据-佣金(结算使用付佣业务数据)完成:{}", type)
    }

    void processSettlement(String type) {
        Map commissionGroup = baseSql.rows(getQueryCommission().replace("#", type)).groupBy {
            it.'保险公司' + it.'省' + it.'order_month' + it.'40-代理人名称'
        }
        List<GroovyRowResult> settlements = baseSql.rows(getQuerySettlement().replace("#", type))
        runThreadPool.submitWithResult(settlements, { it ->
            if ((it.fee as double) > 10) {
                log.info("原数据-结算数据:{}", it)
                matchCommission(it, commissionGroup, type)
            }
        })
    }

    void processCommission(String type) {
        Map settlementGroup = baseSql.rows(getQuerySettlement().replace("#", type)).groupBy {
            it.'保险公司' + it.'省' + it.'order_month'
        }
        List<GroovyRowResult> commissions = baseSql.rows(getQueryCommission().replace("#", type))

        runThreadPool.submitWithResult(commissions, { it ->
            if ((it.commission as double) > 10) {
                log.info("原数据-付佣数据:{}", it)
                matchSettlement(it, settlementGroup, type)
            }
        })
    }

    void matchCommission(GroovyRowResult settlement, Map<String, List<GroovyRowResult>> commissionGroup, String type) {
        for (List<GroovyRowResult> commissions : commissionGroup.values()) {
            if (commissions.size() == 0 || commissions[0].order_month == null || settlement.order_month < commissions[0].order_month || commissions[0].'保险公司' != settlement.'保险公司' || commissions[0].'省' != settlement.'省') {
                continue
            }
            int size = Math.ceil(commissions.size / 10)
            int n = 0
            while (n < size) {
                List<GroovyRowResult> tmp
                if (commissions.size() > 10) {
                    def begin = n * 10
                    def end = (n + 1) * 10
                    end = end > commissions.size ? commissions.size : end
                    tmp = commissions.subList(begin, end)
                } else {
                    tmp = commissions
                }
                n++
                def matchMap = matchData([settlement], tmp, commissions)
                if (matchMap != null) {
                    if (isMatch(matchMap, commissions)) {
                        updateCommissionResult(matchMap, type)
                        return
                    } else {
                        n = 0
                    }
                }
            }
        }
    }

    void matchSettlement(GroovyRowResult commission, Map<String, List<GroovyRowResult>> settlementGroup, String type) {
        for (List<GroovyRowResult> settlements : settlementGroup.values()) {
            if (settlements.size() == 0 || settlements[0].order_month == null || commission.order_month < settlements[0].order_month || settlements[0].'保险公司' != commission.'保险公司' || settlements[0].'省' != commission.'省') {
                continue
            }
            int size = Math.ceil(settlements.size / 10)
            int n = 0
            while (n < size) {
                List<GroovyRowResult> tmp
                if (settlements.size() > 10) {
                    def begin = n * 10
                    def end = (n + 1) * 10
                    end = end > settlements.size ? settlements.size : end
                    tmp = settlements.subList(begin, end)
                } else {
                    tmp = settlements
                }
                n++
                def matchMap = matchData(tmp, [commission], settlements)
                if (matchMap != null) {
                    if (isMatch(matchMap, settlements)) {
                        updateSettlementResult(matchMap, type)
                        return
                    } else {
                        n = 0
                    }
                }
            }
        }
    }

    private static MatchResult<List<GroovyRowResult>, List<GroovyRowResult>> matchData(List<GroovyRowResult> settlements, List<GroovyRowResult> commissions, Object lock) {
        List<GroovyRowResult> commissionTmp, settlementTmp
        synchronized (lock) {
            settlementTmp = settlements.findAll {
                it.flag == null
            }

            commissionTmp = commissions.findAll {
                it.flag == null
            }
        }

        return Utils.matchCombine(settlementTmp, commissionTmp, {
            s, c ->
                def fee = s*.fee.collect { it as double }.sum()
                def commission = c*.commission.collect { it as double }.sum()
                return Math.abs((fee - commission) / fee) < 1
        })
    }

    private boolean isMatch(MatchResult<List<GroovyRowResult>, List<GroovyRowResult>> matchMap, Object lock) {
        synchronized (lock) {
            def key = matchMap.sourceList
            def value = matchMap.targetList
            def tmp1 = key.find { i -> i.flag != null }
            if (tmp1 != null) {
                return false
            }
            def tmp2 = value.find { i -> i.flag != null }
            if (tmp2 != null) {
                return false
            }

            key.each {
                it.flag = 1
            }
            value.each {
                it.flag = 1
            }
            return true
        }
    }

    static String insertRef = "insert into result_gross_margin_ref (table_name,result_id,s_id,c_id,type) values "
    String sTable = "settlement_#"
    String cTable = "commission_#"

    void updateCommissionResult(MatchResult<List<GroovyRowResult>, List<GroovyRowResult>> matchMap, String type) {
        def row
        def c42 = 0, c45 = 0, c46 = 0, sumCommission = 0
        def key = matchMap.sourceList
        def value = matchMap.targetList
        if (key.size() > 1) {
            throw new Exception("只能对一条单边结算进行匹配")
        }
        List valueList = new ArrayList()
        String tableName = "'${getsTable()}'"
        String joinType
        String sId
        key.each { s ->
            row = s
            c42 += (s.'42-佣金金额（已入账）' as double)
            c45 += (s.'45-支付金额' as double)
            c46 += (s.'46-未计提佣金（19年底尚未入帐）' as double)
            sumCommission += (s.'commission' as double)

            if ((row.r_flag as Integer) == 2) {
                sId = (s.c_id as String).split(",")[0]
                joinType = "4"
            } else {
                sId = (s.s_id as String).split(",")[0]
                joinType = "1"
            }

            value.each { c ->
                c42 += (c.'42-佣金金额（已入账）' as double)
                c45 += (c.'45-支付金额' as double)
                c46 += (c.'46-未计提佣金（19年底尚未入帐）' as double)
                sumCommission += (c.'commission' as double)
                (c.c_id as String).split(",").each {
                    StringJoiner values = new StringJoiner(",", "(", ")")
                    values.add(tableName.replace("#", type))
                    values.add("'${s.id}'")
                    values.add("'${sId}'")
                    values.add("'${it}'")
                    values.add(joinType)
                    valueList.add(values)
                }
            }
        }

        baseSql.executeInsert(insertRef + valueList.join(","))
        def cids = value*.id.join(',')
        baseSql.executeUpdate("update ${getsTable()} set handle_sign=4,`42-佣金金额（已入账）`=?,`45-支付金额`=?,`46-未计提佣金（19年底尚未入帐）`=?,sum_commission=?,gross_profit=?,c_id=? where id=?"
                .replace("#", type), [c42, c45, c46, sumCommission, ((row.fee as double) - sumCommission) / (row.fee as double), value*.c_id.join(','), row.id])
        baseSql.executeUpdate("update ${getcTable()} set handle_sign=5 where id in (${cids})".replace("#", type))
        log.info("付佣匹配成功:{}:{} -> {}:{}", getcTable().replace("#", type), cids, getsTable().replace("#", type), row.id)
    }

    void updateSettlementResult(MatchResult<List<GroovyRowResult>, List<GroovyRowResult>> matchMap, String type) {
        def row
        def s14 = 0, s15 = 0, sumFee = 0

        def key = matchMap.sourceList
        def value = matchMap.targetList
        if (value.size() > 1) {
            throw new Exception("只能对一条单边付佣进行匹配")
        }
        List valueList = new ArrayList()
        String tableName = "'${getcTable()}'"
        String joinType = "2"
        String cId
        value.each { c ->
            row = c
            s14 += (c.'14-手续费总额（报行内+报行外）(含税)' as double)
            s15 += (c.'15-手续费总额（报行内+报行外）(不含税)' as double)
            sumFee += (c.'fee' as double)
            if ((row.r_flag as Integer) == 1) {
                cId = (c.s_id as String).split(",")[0]
            } else {
                cId = (c.c_id as String).split(",")[0]
                joinType = "2"
            }
            key.each { s ->
                s14 += (s.'14-手续费总额（报行内+报行外）(含税)' as double)
                s15 += (s.'15-手续费总额（报行内+报行外）(不含税)' as double)
                sumFee += (s.'fee' as double)
                (s.s_id as String).split(",").each {
                    StringJoiner values = new StringJoiner(",", "(", ")")
                    values.add(tableName.replace("#", type))
                    values.add("'${c.id}'")
                    values.add("'${it}'")
                    values.add("'${cId}'")
                    values.add(joinType)
                    valueList.add(values)
                }
            }
        }

        baseSql.executeInsert(insertRef + valueList.join(","))
        def sids = key*.id.join(',')
        baseSql.executeUpdate("update ${getsTable()} set handle_sign=5 where id in (${sids})".replace("#", type))
        baseSql.executeUpdate("update ${getcTable()} set handle_sign=4,`14-手续费总额（报行内+报行外）(含税)`=?,`15-手续费总额（报行内+报行外）(不含税)`=?,sum_fee=?,gross_profit=?,s_id=? where id=?"
                .replace("#", type), [s14, s15, sumFee, (sumFee - (row.commission as double)) / sumFee, key*.s_id.join(','), row.id])

        log.info("结算匹配成功:{}:{} -> {}:{}", getsTable().replace("#", type), sids, getcTable().replace("#", type), row.id)

    }
}
