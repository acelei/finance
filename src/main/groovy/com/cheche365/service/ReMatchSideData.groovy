package com.cheche365.service

import com.cheche365.util.ThreadPoolUtils
import com.google.common.collect.Lists
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service("reMatchSideData")
@Slf4j
class ReMatchSideData {
    @Autowired
    private Sql baseSql

    String errorSettlementSide = "select group_concat(id) as id,group_concat(id) as s_id,sum(sum_fee) as fee,sum(sum_commission) as commission,sum(`14-手续费总额（报行内+报行外）(含税)`) as `14-手续费总额（报行内+报行外）(含税)`,sum(`15-手续费总额（报行内+报行外）(不含税)`) as `15-手续费总额（报行内+报行外）(不含税)`,sum(`42-佣金金额（已入账）`) as `42-佣金金额（已入账）`,sum(`45-支付金额`) as `45-支付金额`,sum(`46-未计提佣金（19年底尚未入帐）`) as `46-未计提佣金（19年底尚未入帐）`,DATE_FORMAT(`9-保单出单日期`,'%Y-%m') as order_month,`保险公司`,`省` from settlement_# where handle_sign=6 and date_format(`9-保单出单日期`,'%Y')='2019' group by `6-保单单号`,`8-险种名称`"
    String errorCommissionSide = "select group_concat(id) as id,group_concat(id) as c_id,sum(sum_fee) as fee,sum(sum_commission) as commission,sum(`14-手续费总额（报行内+报行外）(含税)`) as `14-手续费总额（报行内+报行外）(含税)`,sum(`15-手续费总额（报行内+报行外）(不含税)`) as `15-手续费总额（报行内+报行外）(不含税)`,sum(`42-佣金金额（已入账）`) as `42-佣金金额（已入账）`,sum(`45-支付金额`) as `45-支付金额`,sum(`46-未计提佣金（19年底尚未入帐）`) as `46-未计提佣金（19年底尚未入帐）`,DATE_FORMAT(`9-保单出单日期`,'%Y-%m') as order_month,`保险公司`,`省`,`40-代理人名称` from commission_# where handle_sign=6 and date_format(`9-保单出单日期`,'%Y')='2019' group by `6-保单单号`,`8-险种名称`,`40-代理人名称`"

    void run(String type) {
        log.info("反向匹配结算单边数据:{}-{}", getsTable(), type)
        settlementMatch(type)
        log.info("反向匹配结算单边数据完成:{}-{}", getsTable(), type)
        log.info("反向匹配佣金单边数据:{}-{}", getcTable(), type)
        commissionMatch(type)
        log.info("反向匹配佣金单边数据完成:{}-{}", getcTable(), type)
    }

    void settlementMatch(String type) {
        Map<String, List<GroovyRowResult>> settlementsGroup = baseSql.rows(getErrorSettlementSide().replace("#", type)).groupBy {
            it.'保险公司' + it.'省'
        }

        ThreadPoolUtils.executeRun(Lists.newArrayList(settlementsGroup.values()), { settlements ->
            settlements.each { row ->
                matchSettlementResult(row, type)
            }
        }).await()
    }

    void commissionMatch(String type) {
        Map<String, List<GroovyRowResult>> commissionsGroup = baseSql.rows(getErrorCommissionSide().replace("#", type)).groupBy {
            it.'保险公司' + it.'省' + it.'40-代理人名称'
        }

        ThreadPoolUtils.executeRun(Lists.newArrayList(commissionsGroup.values()), { commissions ->
            commissions.each { row ->
                matchCommissionResult(row, type)
            }
        }).await()
    }

    void matchSettlementResult(GroovyRowResult row, String type) {
        List<GroovyRowResult> resultList = findResult(row, type)

        GroovyRowResult result
        for (GroovyRowResult i : resultList) {
            def r = ((i.fee as double) + (row.fee as double) - (i.commission as double)) / ((i.fee as double) + (row.fee as double))
            if (r > -1 && r < 1) {
                result = i
                break
            }
        }
        if (result != null) {
            updateSettlement(row, result, type)
            log.info("结算匹配成功:{}-{} -> {}-{}", getsTable().replace("#", type), row.id, "result_${type}_2", result.id)
        }
    }

    void matchCommissionResult(GroovyRowResult row, String type) {
        List<GroovyRowResult> resultList = findResult(row, type)

        GroovyRowResult result
        for (GroovyRowResult i : resultList) {
            def r = ((i.fee as double) - (i.commission as double) - (row.commission as double)) / (i.fee as double)
            if (r > -1 && r < 1) {
                result = i
                break
            }
        }
        if (result != null) {
            updateCommission(row, result, type)
            log.info("付佣匹配成功:{}-{} -> {}-{}", getcTable().replace("#", type), row.id, "result_${type}_2", result.id)
        }
    }

    String quernSettlementUp = '''
select id,s_id,c_id,sum_fee  as fee,
       sum_commission as commission,
       `14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,
       `42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`
from result_#_2
where handle_sign in (0, 1, 3, 4, 6)
  and (abs(ifnull(0+`11-净保费`, 0))*0.7)-sum_fee > ?
  and (abs(ifnull(0+`11-净保费`, 0))*if(`8-险种名称` = '交强险', 0, 0.12)) - sum_fee < ?
  and DATE_FORMAT(`9-保单出单日期`,'%Y-%m') <= ?
  and `保险公司` = ?
  and `省` = ?
  and `8-险种名称` in ('交强险','商业险')
order by gross_profit
limit 100
'''
    String quernSettlementDown = '''
select id,s_id,c_id,sum_fee  as fee,
       sum_commission as commission,
       `14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,
       `42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`
from result_#_2
where handle_sign in (0, 1, 3, 4, 6)
  and sum_fee>0
  and sum_fee-(ifnull(0+`11-净保费`, 0)*if(`8-险种名称` = '交强险', 0, 0.12))> ?
  and DATE_FORMAT(`9-保单出单日期`,'%Y-%m') <= ?
  and `保险公司` = ?
  and `省` = ?
  and `8-险种名称` in ('交强险','商业险')
order by gross_profit desc
limit 100
'''
    String quernCommissionUp = '''
select id,s_id,c_id,sum_fee  as fee,
       sum_commission as commission,
       `14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,
       `42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`
from result_#_2
where handle_sign in (0, 1, 3, 4, 6)
  and (abs(ifnull(0+`11-净保费`, 0))*0.7)-sum_commission > ?
  and 0-sum_commission < ?
  and `40-代理人名称`=?
  and DATE_FORMAT(`9-保单出单日期`,'%Y-%m') <= ?
  and `保险公司` = ?
  and `省` = ?
  and `8-险种名称` in ('交强险','商业险')
order by gross_profit desc
limit 100
'''
    String quernCommissionDown = '''
select id,s_id,c_id,sum_fee  as fee,
       sum_commission as commission,
       `14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,
       `42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`
from result_#_2
where handle_sign in (0, 1, 3, 4, 6)
  ans sum_commission>0
  and sum_commission-(ifnull(0+`11-净保费`, 0)*if(`8-险种名称` = '交强险', 0, 0.12)) > ?
  and `40-代理人名称`=?
  and DATE_FORMAT(`9-保单出单日期`,'%Y-%m') <= ?
  and `保险公司` = ?
  and `省` = ?
  and `8-险种名称` in ('交强险','商业险')
order by sum_commission desc,
         gross_profit
limit 100
'''

    List<GroovyRowResult> findResult(GroovyRowResult row, String type) {
        def fee = row.fee as double
        def commission = row.commission as double
        if (fee > 0) {
            return baseSql.rows(getQuernSettlementUp().replace("#", type), [fee, fee, row.'order_month', row.'保险公司', row.'省'])
        }

        if (fee < 0) {
            return baseSql.rows(getQuernSettlementDown().replace("#", type), [0 - fee, row.'order_month', row.'保险公司', row.'省'])
        }

        if (commission > 0) {
            return baseSql.rows(getQuernCommissionUp().replace("#", type), [commission, commission, row.'40-代理人名称', row.'order_month', row.'保险公司', row.'省'])
        }

        if (commission < 0) {
            return baseSql.rows(getQuernCommissionDown().replace("#", type), [0 - commission, row.'40-代理人名称', row.'order_month', row.'保险公司', row.'省'])
        }
    }

    static String insertRef = "insert into result_gross_margin_ref (table_name,result_id,s_id,c_id,type) values "
    String sTable = "settlement_#"
    String cTable = "commission_#"

    void updateSettlement(GroovyRowResult row, GroovyRowResult result, String type) {
        def cId = (result.c_id as String).split(",")[0]
        def sIds = (row.s_id as String).split(",")
        String tableName = "'${getcTable()}'"
        String joinType = "2"

        List valueList = new ArrayList()
        sIds.each {
            StringJoiner values = new StringJoiner(",", "(", ")")
            values.add(tableName.replace("#", type))
            values.add(row.id as String)
            values.add(it)
            values.add(cId)
            values.add(joinType)
            valueList.add(values)
        }

        def s1 = (result.'14-手续费总额（报行内+报行外）(含税)' as double) + (row.'14-手续费总额（报行内+报行外）(含税)' as double)
        def s2 = (result.'15-手续费总额（报行内+报行外）(不含税)' as double) + (row.'15-手续费总额（报行内+报行外）(不含税)' as double)
        def fee = (result.fee as double) + (row.fee as double)

        baseSql.executeInsert(insertRef + valueList.join(","))
        baseSql.executeUpdate("update ${getsTable()} set handle_sign=5 where id in (${row.id})".replace("#", type))
        baseSql.executeUpdate("update result_#_2 set handle_sign=4,`14-手续费总额（报行内+报行外）(含税)`=?,`15-手续费总额（报行内+报行外）(不含税)`=?,sum_fee=?,gross_profit=?,s_id=? where id=?".replace("#", type),
                [s1, s2, fee, (fee - (result.commission as double)) / fee, "${result.s_id},${row.s_id}" as String, result.id])
    }

    void updateCommission(GroovyRowResult row, GroovyRowResult result, String type) {
        def sId = (result.s_id as String).split(",")[0]
        def cIds = (row.c_id as String).split(",")

        String tableName = "'${getsTable()}'"
        String joinType = "1"

        List valueList = new ArrayList()
        cIds.each {
            StringJoiner values = new StringJoiner(",", "(", ")")
            values.add(tableName.replace("#", type))
            values.add(row.id as String)
            values.add(sId)
            values.add(it)
            values.add(joinType)
            valueList.add(values)
        }
        def c1 = (result.'42-佣金金额（已入账）' as double) + (row.'42-佣金金额（已入账）' as double)
        def c2 = (result.'45-支付金额' as double) + (row.'45-支付金额' as double)
        def c3 = (result.'46-未计提佣金（19年底尚未入帐）' as double) + (row.'46-未计提佣金（19年底尚未入帐）' as double)
        def commission = (result.commission as double) + (row.commission as double)

        baseSql.executeInsert(insertRef + valueList.join(","))
        baseSql.executeUpdate("update result_#_2 set handle_sign=4,`42-佣金金额（已入账）`=?,`45-支付金额`=?,`46-未计提佣金（19年底尚未入帐）`=?,sum_commission=?,gross_profit=?,c_id=? where id =?".replace("#", type),
                [c1, c2, c3, commission, ((result.fee as double) - commission) / (result.fee as double), "${result.c_id},${row.c_id}" as String, result.id])
        baseSql.executeUpdate("update ${getcTable()} set handle_sign=5 where id in (${row.id})".replace("#", type))
    }
}
