package com.cheche365.service

import com.cheche365.util.ThreadPoolUtils
import com.cheche365.util.Utils
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
@Slf4j
class FixProfit {
    @Autowired
    Sql baseSql

    private static final String querySql = '''
 select * ,
       gross_profit as grossMargin
       from (
        select id,
       s_id                                                as sIds,
       c_id                                                as cIds,
       ifnull(`11-净保费`,0) as premium,
       sum_fee                            as sumFee,
       sum_commission as sumCommission,
       gross_profit
       from `result_#_2`
       where 
       `8-险种名称` in ('交强险','商业险')
       and c_id is not null
       and s_id is not null
       and handle_sign = 0
       and date_format(`9-保单出单日期`,'%Y')='2019'
       ) as temp
       where AbS(gross_profit) > 1
'''

    void fixSettlementCommission(String type) {
        log.info("调整毛利率异常数据:{}", type)
        List<GroovyRowResult> dataList = baseSql.rows(querySql.replace("#", type))
        ThreadPoolUtils.submitRun(dataList, {
//            handleSettlementCommission(it, type)
            handleGrossMargin(it, type)
        }).each {it.get()}
        log.info("调整毛利率异常数据完成:{}", type)
    }

    @Deprecated
    void handleSettlementCommission(GroovyRowResult row, String type) {
        if ((row.grossMargin as double) < -1) {
            handleGrossCommissionMargin(row, type)
        }

        if ((row.grossMargin as double) > 1) {
            handleGrossSettlementMargin(row, type)
        }
    }

    private static final String commissionSql = "select id, sum_commission as commission,`42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）` from commission_# where id in "
    private static final String updateCommissionResult = "update result_#_2 set c_id=?,handle_sign=1 where id=?"
    private static final String updateCommission = "update commission_# set d_id=4,handle_sign=6 where id in "

    private static final String settlementSql = "select id,sum_fee as fee,`14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)` from settlement_# where id in "
    private static final String updateSettlementResult = "update result_#_2 set s_id=?,handle_sign=1 where id=?"
    private static final String updateSettlement = "update settlement_# set d_id=4,handle_sign=6 where id in "

    private static final String updateResult = "update result_#_2 set s_id=?,c_id=?,`14-手续费总额（报行内+报行外）(含税)`=?,`15-手续费总额（报行内+报行外）(不含税)`=?,`42-佣金金额（已入账）`=?,`45-支付金额`=?,`46-未计提佣金（19年底尚未入帐）`=?,sum_fee=?,sum_commission=?,gross_profit=?,handle_sign=1 where id=?"
    private static final String errFlag = "update result_#_2 set handle_sign=2 where id = ?"

    @Deprecated
    private void handleGrossCommissionMargin(GroovyRowResult row, String type) {
        List dataList = baseSql.rows(commissionSql.replace("#", type) + "(${row.cIds})")

        List resultList = Utils.combine(dataList, { i ->
            def commission = i*.commission.collect { it as double }.sum()
            def fee = row.sumFee as double
            def r = (fee - commission) / fee
            r > -1 && r < 1
        })

        resultList = resultList ?: []
        List errorList = dataList - resultList
        if (errorList.size() > 0 && resultList.size() > 0) {
            baseSql.executeUpdate(updateCommissionResult.replace('#', type), resultList*.id.join(','), row.id)
            baseSql.executeUpdate(updateCommission.replace("#", type) + "(" + errorList*.id.join(",") + ")")
        } else {
            baseSql.executeUpdate(errFlag.replace('#', type), [row.id])
        }
    }

    @Deprecated
    private void handleGrossSettlementMargin(GroovyRowResult row, String type) {
        List dataList = baseSql.rows(settlementSql.replace("#", type) + "(${row.sIds})")

        List resultList = Utils.combine(dataList, { i ->
            def fee = i*.fee.collect { it as double }.sum()
            def commission = row.sumCommission as double
            def r = (fee - commission) / fee
            r > -1 && r < 1
        })

        resultList = resultList ?: []
        List errorList = dataList - resultList
        if (errorList.size() > 0 && resultList.size() > 0) {
            baseSql.executeUpdate(updateSettlementResult.replace('#', type), resultList*.id.join(','), row.id)
            baseSql.executeUpdate(updateSettlement.replace("#", type) + "(" + errorList*.id.join(",") + ")")
        } else {
            baseSql.executeUpdate(errFlag.replace('#', type), [row.id])
        }
    }

    void handleGrossMargin(GroovyRowResult row, String type) {
        List settlementList = baseSql.rows(settlementSql.replace("#", type) + "(${row.sIds})")
        List commissionList = baseSql.rows(commissionSql.replace("#", type) + "(${row.cIds})")
        def premium = row.premium as double

        Map map = Utils.matchCombine(settlementList, commissionList, { s, c ->
            def fee = s*.fee.collect { it as double }.sum()
            def commission = c*.commission.collect { it as double }.sum()
            def r = (fee - commission) / fee
            (r > -1 && r < 1) || (fee > 0 && commission > 0 && fee < premium * 0.7 && commission < premium * 0.7)
        })

        if (map != null) {
            map.each { key, value ->
                def s1 = key*.'14-手续费总额（报行内+报行外）(含税)'.collect { it as double }.sum()
                def s2 = key*.'15-手续费总额（报行内+报行外）(不含税)'.collect { it as double }.sum()
                def fee = key*.fee.collect { it as double }.sum()

                def c1 = value*.'42-佣金金额（已入账）'.collect { it as double }.sum()
                def c2 = value*.'45-支付金额'.collect { it as double }.sum()
                def c3 = value*.'46-未计提佣金（19年底尚未入帐）'.collect { it as double }.sum()
                def commission = value*.commission.collect { it as double }.sum()

                baseSql.executeUpdate(updateResult.replace('#', type), key*.id.join(','), value*.id.join(','), s1, s2, c1, c2, c3, fee, commission, (fee - commission) / fee, row.id)

                List errorList = settlementList - key
                if (errorList.size() > 0) {
                    baseSql.executeUpdate(updateSettlement.replace("#", type) + "(" + errorList*.id.join(",") + ")")
                }
                errorList = commissionList - value
                if (errorList.size() > 0) {
                    baseSql.executeUpdate(updateCommission.replace("#", type) + "(" + errorList*.id.join(",") + ")")
                }
            }
        } else {
            baseSql.executeUpdate(errFlag.replace('#', type), [row.id])
        }
    }
}
