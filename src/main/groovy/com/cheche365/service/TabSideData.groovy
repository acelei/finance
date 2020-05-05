package com.cheche365.service

import com.cheche365.util.ThreadPool
import com.cheche365.util.Utils
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

import java.util.concurrent.CopyOnWriteArrayList

@Service
@Slf4j
class TabSideData {
    @Autowired
    private Sql baseSql
    @Autowired
    private ThreadPool runThreadPool

    String sideFlag1 = "update result_#_2 set handle_sign=6,r_flag=1 where c_id is null"
    String sideFlag2 = "update result_#_2 set handle_sign=6,r_flag=2 where s_id is null"

    void setDefaultSideFlag(String type) {
        baseSql.executeUpdate(sideFlag1.replace("#", type), )
        baseSql.executeUpdate(sideFlag2.replace("#", type), )
    }

    private static final String errSettlementSql = "select id,s_id,`11-净保费` from result_#_2 where handle_sign=6 and c_id is null and `8-险种名称` in ('交强险','商业险') and (sum_fee/`11-净保费` <0 or sum_fee/`11-净保费`>0.7) and date_format(`9-保单出单日期`,'%Y')='2019'"
    private static final String settlementSql = "select id,sum_fee as fee,`14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,sum_fee as fee from settlement_# where id in "
    private static final String errCommissionSql = "select id,c_id,`11-净保费` from result_#_2 where handle_sign=6 and s_id is null and `8-险种名称` in ('交强险','商业险') and (sum_commission/`11-净保费` <0 or sum_commission/`11-净保费`>0.7) and date_format(`9-保单出单日期`,'%Y')='2019'"
    private static final String commissionSql = "select id,sum_commission as commission,`42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,sum_commission as commission from commission_# where id in "

    /**
     * 单收入且不能匹配中调整以保证保费以手续费比例正常
     * 剔除数据在settlement中标记d_id为5
     * @param type
     */
    void tabSettlement(String type) {
        log.info("根据保费与手续费调整比例比正常数据-结算:{}", type)
        List rows = baseSql.rows(errSettlementSql.replace("#", type))

        runThreadPool.submitWithResult(rows, {
            setSettlementValue(it, type)
        })
        log.info("根据保费与手续费调整比例比正常数据完成-结算:{}", type)
    }

    private static final String updateSettlementResult = "update result_#_2 set s_id=?,`14-手续费总额（报行内+报行外）(含税)`=?,`15-手续费总额（报行内+报行外）(不含税)`=?,sum_fee=? where id=?"
    private static final String updateSettlement = "update settlement_# set d_id=5,handle_sign=6 where id in "

    void setSettlementValue(GroovyRowResult row, String type) {
        def premium = row.'11-净保费' as double
        List settlements = baseSql.rows(settlementSql.replace('#', type) + '(' + row.'s_id' + ')')

        int n = settlements.size()
        if (n <= 1) {
            return
        }

        List resultList = Utils.combine(settlements, { i ->
            def fee = i*.fee.collect { it as double }.sum()
            def r = fee / premium
            r > 0 && r < 0.7
        })

        resultList = resultList ?: []
        List errorList = settlements - resultList
        if (errorList.size() > 0 && resultList.size() > 0) {
            def s1 = resultList*.'14-手续费总额（报行内+报行外）(含税)'.collect { it as double }.sum()
            def s2 = resultList*.'15-手续费总额（报行内+报行外）(不含税)'.collect { it as double }.sum()
            def fee = resultList*.fee.collect { it as double }.sum()

            baseSql.executeUpdate(updateSettlementResult.replace('#', type), resultList*.id.join(','), s1, s2, fee, row.id)
            baseSql.executeUpdate(updateSettlement.replace("#", type) + "(" + errorList*.id.join(",") + ")")
        }
    }

    void tabCommission(String type) {
        log.info("根据保费与手续费调整比例比正常数据-佣金:{}", type)
        List rows = baseSql.rows(errCommissionSql.replace("#", type))

        runThreadPool.submitWithResult(rows, {
            setCommissionValue(it, type)
        })
        log.info("根据保费与手续费调整比例比正常数据完成-佣金:{}", type)
    }

    private static final String updateCommissionResult = "update result_#_2 set c_id=?,`42-佣金金额（已入账）`=?,`45-支付金额`=?,`46-未计提佣金（19年底尚未入帐）`=?,sum_commission=? where id=?"
    private static final String updateCommission = "update commission_# set d_id=5,handle_sign=6 where id in "

    void setCommissionValue(GroovyRowResult row, String type) {
        def premium = row.'11-净保费' as double
        List settlements = baseSql.rows(commissionSql.replace('#', type) + '(' + row.'c_id' + ')')

        int n = settlements.size()
        if (n <= 1) {
            return
        }

        List resultList = Utils.combine(settlements, { i ->
            def commission = i*.commission.collect { it as double }.sum()
            def r = commission / premium
            r > 0 && r < 0.7
        })

        resultList = resultList ?: []
        List errorList = settlements - resultList
        if (errorList.size() > 0 && resultList.size() > 0) {
            def c1 = resultList*.'42-佣金金额（已入账）'.collect { it as double }.sum()
            def c2 = resultList*.'45-支付金额'.collect { it as double }.sum()
            def c3 = resultList*.'46-未计提佣金（19年底尚未入帐）'.collect { it as double }.sum()
            def commission = resultList*.commission.collect { it as double }.sum()

            baseSql.executeUpdate(updateCommissionResult.replace('#', type), resultList*.id.join(','), c1, c2, c3, commission, row.id)
            baseSql.executeUpdate(updateCommission.replace("#", type) + "(" + errorList*.id.join(",") + ")")
        }
    }

    String resultSide1 = '''
select id,s_id, c_id from result_#_2 where  handle_sign != 5 and `8-险种名称` in ('交强险','商业险') and date_format(`9-保单出单日期`,'%Y')='2019' and round(sum_fee,2) < 0
union all
select id,s_id, c_id from result_#_2 where  handle_sign != 5 and `8-险种名称` in ('交强险','商业险') and date_format(`9-保单出单日期`,'%Y')='2019' and round(sum_commission,2) < 0
'''

    String resultSide2 = '''
select id,s_id, c_id from result_#_2 where  handle_sign != 5 and `8-险种名称` in ('交强险','商业险') and handle_sign != 3 and date_format(`9-保单出单日期`,'%Y')='2019' and 0+sum_fee > 0+sum_commission and (sum_fee / abs(`11-净保费`)) < if(`8-险种名称` = '交强险', 0, 0.12)
union all
select id,s_id, c_id from result_#_2 where  handle_sign != 5 and `8-险种名称` in ('交强险','商业险') and handle_sign != 3 and date_format(`9-保单出单日期`,'%Y')='2019' and 0+sum_fee > 0+sum_commission and (sum_fee / abs(`11-净保费`)) > 0.7
union all
select id,s_id, c_id from result_#_2 where  handle_sign != 5 and `8-险种名称` in ('交强险','商业险') and handle_sign != 3 and date_format(`9-保单出单日期`,'%Y')='2019' and 0+sum_fee < 0+sum_commission and (sum_commission / abs(`11-净保费`)) > 0.7
'''

    String resultSide3 = '''
select id,s_id, c_id from result_#_2 where  handle_sign != 5 and `8-险种名称` in ('交强险','商业险') and handle_sign != 3 and date_format(`9-保单出单日期`,'%Y')='2019' and 0+sum_fee > 0+sum_commission and (sum_fee / abs(`11-净保费`)) < 0
union all
select id,s_id, c_id from result_#_2 where  handle_sign != 5 and `8-险种名称` in ('交强险','商业险') and handle_sign != 3 and date_format(`9-保单出单日期`,'%Y')='2019' and 0+sum_fee > 0+sum_commission and (sum_fee / abs(`11-净保费`)) > 0.7
union all
select id,s_id, c_id from result_#_2 where  handle_sign != 5 and `8-险种名称` in ('交强险','商业险') and handle_sign != 3 and date_format(`9-保单出单日期`,'%Y')='2019' and 0+sum_fee < 0+sum_commission and (sum_commission / abs(`11-净保费`)) > 0.7
'''
    String downSettlement = "update settlement_# set d_id=5,handle_sign=6 where id in (:ids)"
    String downCommission = "update commission_# set d_id=5,handle_sign=6 where id in (:ids)"
    //
    String updateResult = "update result_#_2 set handle_sign=7 where id=?"

    void putDownFlag1(String type) {
        log.info("下放收入成本为负数的标志位:{}", type)
        putDownFlag(type, resultSide1)
        log.info("下放收入成本为负数的标志位完成:{}", type)
    }

    List<String> zTypes = ["guangxi", "shanxi_baodai", "shanxi_keji", "yx", "hubei_czl_keji"]

    void putDownFlag2(String type) {
        log.info("下放收入成本与保费比例异常的标志位:{}", type)
        if (zTypes.contains(type)) {
            putDownFlag(type, resultSide3)

        } else {
            putDownFlag(type, resultSide2)
        }

        log.info("下放收入成本与保费比例异常的标志位完成:{}", type)
    }

    void putDownFlag(String type, String sql) {
        def rows = baseSql.rows(sql.replace("#", type))
        List<String> ids = runThreadPool.submitWithResult(rows, { row ->
            if (row.s_id != null) {
                baseSql.executeUpdate(downSettlement.replace("#", type).replace(":ids", row.s_id as String))
            }
            if (row.c_id != null) {
                baseSql.executeUpdate(downCommission.replace("#", type).replace(":ids", row.c_id as String))
            }
            baseSql.executeUpdate(updateResult.replace("#", type), [row.id])
            return ("'${row.id}'" as String)
        })

        if (ids.size() > 0) {
            baseSql.executeUpdate("delete from result_gross_margin_ref where table_name='result_${type}_2' and result_id in (${ids.join(",")})" as String)
        }
    }
}