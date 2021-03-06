package com.cheche365.service

import com.cheche365.util.ThreadPool
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service("reMatchSideData")
@Slf4j
class ReMatchSideData {
    @Autowired
    Sql baseSql
    @Autowired
    private ThreadPool runThreadPool

    String errorSettlementSide = "select group_concat(id) as id,group_concat(id) as s_id,sum(sum_fee) as fee,sum(sum_commission) as commission,sum(`14-手续费总额（报行内+报行外）(含税)`) as `14-手续费总额（报行内+报行外）(含税)`,sum(`15-手续费总额（报行内+报行外）(不含税)`) as `15-手续费总额（报行内+报行外）(不含税)`,sum(`42-佣金金额（已入账）`) as `42-佣金金额（已入账）`,sum(`45-支付金额`) as `45-支付金额`,sum(`46-未计提佣金（19年底尚未入帐）`) as `46-未计提佣金（19年底尚未入帐）`,DATE_FORMAT(`9-保单出单日期`,'%Y-%m') as order_month,`保险公司`,`省`,`4-发票付款方（与发票一致）` from settlement_# where `8-险种名称` in ('交强险','商业险') and handle_sign=6 and date_format(`9-保单出单日期`,'%Y')>='2019' group by `6-保单单号`,`8-险种名称`"
    String errorCommissionSide = "select group_concat(id) as id,group_concat(id) as c_id,sum(sum_fee) as fee,sum(sum_commission) as commission,sum(`14-手续费总额（报行内+报行外）(含税)`) as `14-手续费总额（报行内+报行外）(含税)`,sum(`15-手续费总额（报行内+报行外）(不含税)`) as `15-手续费总额（报行内+报行外）(不含税)`,sum(`42-佣金金额（已入账）`) as `42-佣金金额（已入账）`,sum(`45-支付金额`) as `45-支付金额`,sum(`46-未计提佣金（19年底尚未入帐）`) as `46-未计提佣金（19年底尚未入帐）`,DATE_FORMAT(`9-保单出单日期`,'%Y-%m') as order_month,`保险公司`,`省`,`40-代理人名称` from commission_# where `8-险种名称` in ('交强险','商业险') and handle_sign=6 and date_format(`9-保单出单日期`,'%Y')>='2019' group by `6-保单单号`,`8-险种名称`,`40-代理人名称`"

    void run(String type) {
        log.info("反向匹配结算单边数据:{}-{}", getsTable(), type)
        settlementMatch(type)
        log.info("反向匹配结算单边数据完成:{}-{}", getsTable(), type)
        log.info("反向匹配佣金单边数据:{}-{}", getcTable(), type)
        commissionMatch(type)
        log.info("反向匹配佣金单边数据完成:{}-{}", getcTable(), type)
    }

    void settlementMatch(String type) {
        def rows = baseSql.rows(getErrorSettlementSide().replace("#", type))

        runThreadPool.submitWithResult(rows, { row ->
            matchSettlementResult(row, type)
        })
    }

    void commissionMatch(String type) {
        def rows = baseSql.rows(getErrorCommissionSide().replace("#", type))

        runThreadPool.submitWithResult(rows, { row ->
            matchCommissionResult(row, type)
        })
    }

    void matchSettlementResult(GroovyRowResult row, String type) {
        boolean f = false
        while (!f) {
            List<GroovyRowResult> resultList = findResult(row, type)
            if (resultList == null || resultList.size() == 0) {
                break
            }

            GroovyRowResult result
            for (GroovyRowResult i : resultList) {
                def premium = i.premium as double
                def fee = (i.fee as double) + (row.fee as double)
                def commission = (i.commission as double)
                def r = 0
                if (fee != 0 && commission != 0) {
                    r = (fee - commission) / fee
                }
                if ((r > -1 && r < 1) || (fee > 0 && commission > 0 && fee < premium * 0.7 && commission < premium * 0.7)) {
                    result = i
                    break
                }
            }
            if (result != null) {
                try {
                    f = updateSettlement(row, result, type)
                    if (f) {
                        log.info("结算匹配成功:{}-{} -> {}-{}", getsTable().replace("#", type), row.id, "result_${type}_2", result.id)
                    }
                } catch (e) {
                    log.error("type:{}", type)
                    log.error("row:{}", row)
                    log.error("result:{}", result)
                    log.error("", e)
                    f = true
                }
            } else {
                f = true
            }
        }
    }

    void matchCommissionResult(GroovyRowResult row, String type) {
        boolean f = false
        while (!f) {
            List<GroovyRowResult> resultList = findResult(row, type)
            if (resultList == null || resultList.size() == 0) {
                break
            }

            GroovyRowResult result
            for (GroovyRowResult i : resultList) {
                def premium = i.premium as double
                def fee = (i.fee as double)
                def commission = (i.commission as double) + (row.commission as double)
                def r = 0
                if (fee != 0 && commission != 0) {
                    r = (fee - commission) / fee
                }
                if ((r > -1 && r < 1) || (fee > 0 && commission > 0 && fee < premium * 0.7 && commission < premium * 0.7)) {
                    result = i
                    break
                }
            }
            if (result != null) {
                try {
                    f = updateCommission(row, result, type)
                    if (f) {
                        log.info("付佣匹配成功:{}-{} -> {}-{}", getcTable().replace("#", type), row.id, "result_${type}_2", result.id)
                    }
                } catch (e) {
                    log.error("type:{}", type)
                    log.error("row:{}", row)
                    log.error("result:{}", result)
                    log.error("错误信息", e)
                    f = true
                }
            } else {
                f = true
            }
        }
    }

    String quernSettlementUp = '''
select id,s_id,c_id,sum_fee  as fee,
       sum_commission as commission,
       `11-净保费` as premium,
       `14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,
       `42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,
       version,
       r_flag,
       handle_sign
from result_#_2
where handle_sign in (0, 1, 3, 4, 6, 9, 10)
  and (abs(0+`11-净保费`)*0.7)-sum_fee > ?
  and (abs(0+`11-净保费`)*if(`8-险种名称` = '交强险', 0, 0.12)) - sum_fee < ?
  and DATE_FORMAT(`9-保单出单日期`,'%Y-%m') <= ?
  and `保险公司` = ?
  and `省` = ?
  and if(s_id is null, 1, `4-发票付款方（与发票一致）`) = if(s_id is null, 1, ?)
  and `8-险种名称` in ('交强险','商业险')
order by rand()
limit 100
'''
    String quernSettlementDown = '''
select id,s_id,c_id,sum_fee  as fee,
       sum_commission as commission,
       `11-净保费` as premium,
       `14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,
       `42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,
       version,
       r_flag,
       handle_sign
from result_#_2
where handle_sign in (0, 1, 3, 4, 6, 9, 10)
  and sum_fee-(abs(0+`11-净保费`)*if(`8-险种名称` = '交强险', 0, 0.12))> ?
  and DATE_FORMAT(`9-保单出单日期`,'%Y-%m') <= ?
  and `保险公司` = ?
  and `省` = ?
  and `4-发票付款方（与发票一致）` = ?
  and `8-险种名称` in ('交强险','商业险')
order by rand()
limit 100
'''
    String quernCommissionUp = '''
select id,s_id,c_id,sum_fee  as fee,
       sum_commission as commission,
       `11-净保费` as premium,
       `14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,
       `42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,
       version,
       r_flag,
       handle_sign
from result_#_2
where handle_sign in (0, 1, 3, 4, 6, 9, 10)
  and (abs(0+`11-净保费`)*0.7)-sum_commission > ?
  and 0-sum_commission < ?
  and if(c_id is null, 1, `40-代理人名称`)=if(c_id is null, 1, ?)
  and DATE_FORMAT(`9-保单出单日期`,'%Y-%m') <= ?
  and `保险公司` = ?
  and `省` = ?
  and `8-险种名称` in ('交强险','商业险')
order by rand()
limit 100
'''
    String quernCommissionDown = '''
select id,s_id,c_id,sum_fee  as fee,
       sum_commission as commission,
       `11-净保费` as premium,
       `14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,
       `42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,
       version,
       r_flag,
       handle_sign
from result_#_2
where handle_sign in (0, 1, 3, 4, 6, 9, 10)
  and 0+sum_commission > ?
  and `40-代理人名称`=?
  and DATE_FORMAT(`9-保单出单日期`,'%Y-%m') <= ?
  and `保险公司` = ?
  and `省` = ?
  and `8-险种名称` in ('交强险','商业险')
order by rand()
limit 100
'''

    List<GroovyRowResult> findResult(GroovyRowResult row, String type) {
        def fee = row.fee as double
        def commission = row.commission as double
        if (fee > 0) {
            return baseSql.rows(getQuernSettlementUp().replace("#", type), [fee, fee, row.'order_month', row.'保险公司', row.'省' , row.'4-发票付款方（与发票一致）'])
        }

        if (fee < 0) {
            return baseSql.rows(getQuernSettlementDown().replace("#", type), [0 - fee, row.'order_month', row.'保险公司', row.'省', row.'4-发票付款方（与发票一致）'])
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

    boolean updateSettlement(GroovyRowResult row, GroovyRowResult result, String type) {
        def s1 = (result.'14-手续费总额（报行内+报行外）(含税)' as double) + (row.'14-手续费总额（报行内+报行外）(含税)' as double)
        def s2 = (result.'15-手续费总额（报行内+报行外）(不含税)' as double) + (row.'15-手续费总额（报行内+报行外）(不含税)' as double)
        def fee = (result.fee as double) + (row.fee as double)
        def handleSign = 4
        if ((result.handle_sign as Integer) == 3) {
            handleSign = 3
        }

        def i = baseSql.executeUpdate("update result_#_2 set handle_sign=?,`14-手续费总额（报行内+报行外）(含税)`=?,`15-手续费总额（报行内+报行外）(不含税)`=?,sum_fee=?,gross_profit=?,s_id=?,version=? where id=? and version=?".replace("#", type),
                [handleSign, s1, s2, fee, (fee - (result.commission as double)) / fee, "${result.s_id},${row.s_id}".replace("null,", "").replace(",null", ""), result.version + 1, result.id, result.version])

        if (i == 0) {
            return false
        }
        String cId, tableName, joinType
        String[] sIds = (row.s_id as String).split(",")
        if (result.c_id == null || (result.r_flag as Integer) == 1) {
            cId = (result.s_id as String).split(",")[0]
            tableName = "'${getsTable()}'"
            joinType = "3"
        } else {
            cId = (result.c_id as String).split(",")[0]
            tableName = "'${getcTable()}'"
            joinType = "2"
        }

        List valueList = new ArrayList()
        sIds.each {
            StringJoiner values = new StringJoiner(",", "(", ")")
            values.add(tableName.replace("#", type))
            values.add("'${result.id}'")
            values.add("'${it}'")
            values.add("'${cId}'")
            values.add(joinType)
            valueList.add(values)
        }

        baseSql.executeInsert(insertRef + valueList.join(","))
        baseSql.executeUpdate("update ${getsTable()} set handle_sign=5 where id in (${row.id})".replace("#", type))
        return true
    }

    boolean updateCommission(GroovyRowResult row, GroovyRowResult result, String type) {
        def c1 = (result.'42-佣金金额（已入账）' as double) + (row.'42-佣金金额（已入账）' as double)
        def c2 = (result.'45-支付金额' as double) + (row.'45-支付金额' as double)
        def c3 = (result.'46-未计提佣金（19年底尚未入帐）' as double) + (row.'46-未计提佣金（19年底尚未入帐）' as double)
        def commission = (result.commission as double) + (row.commission as double)
        def r = null
        if ((result.fee as double) != 0) {
            r = (((result.fee as double) - commission) / (result.fee as double))
        }
        def handleSign = 4
        if ((result.handle_sign as Integer) == 3) {
            handleSign = 3
        }
        def i = baseSql.executeUpdate("update result_#_2 set handle_sign=?,`42-佣金金额（已入账）`=?,`45-支付金额`=?,`46-未计提佣金（19年底尚未入帐）`=?,sum_commission=?,gross_profit=?,c_id=?,version=? where id =? and version=?".replace("#", type),
                [handleSign, c1, c2, c3, commission, r, "${result.c_id},${row.c_id}".replace("null,", "").replace(",null", ""), result.version + 1, result.id, result.version])

        if (i == 0) {
            return false
        }
        String sId, tableName, joinType
        String[] cIds = (row.c_id as String).split(",")
        if (result.s_id == null || (result.r_flag as Integer) == 2) {
            sId = (result.c_id as String).split(",")[0]
            tableName = "'${getcTable()}'"
            joinType = "4"
        } else {
            sId = (result.s_id as String).split(",")[0]
            tableName = "'${getsTable()}'"
            joinType = "1"
        }

        List valueList = new ArrayList()
        cIds.each {
            StringJoiner values = new StringJoiner(",", "(", ")")
            values.add(tableName.replace("#", type))
            values.add("'${result.id}'")
            values.add("'${sId}'")
            values.add("'${it}'")
            values.add(joinType)
            valueList.add(values)
        }

        baseSql.executeInsert(insertRef + valueList.join(","))
        baseSql.executeUpdate("update ${getcTable()} set handle_sign=5 where id in (${row.id})".replace("#", type))
        return true
    }
}
