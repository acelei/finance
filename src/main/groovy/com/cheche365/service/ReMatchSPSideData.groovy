package com.cheche365.service

import groovy.sql.GroovyRowResult
import org.springframework.stereotype.Service

@Service("reMatchSPSideData")
class ReMatchSPSideData extends ReMatchSideData {
    String quernCommissionUp = '''
select id,s_id,c_id,sum_fee  as fee,
       sum_commission as commission,
       `11-净保费` as premium,
       `14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,
       `42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`
from result_#_2
where handle_sign in (0, 1, 3, 4, 6)
  and abs(0+`11-净保费`)-sum_commission > ?
  and 0-sum_commission < ?
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
       `11-净保费` as premium,
       `14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,
       `42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`
from result_#_2
where handle_sign in (0, 1, 3, 4, 6)
  and sum_commission > ?
  and DATE_FORMAT(`9-保单出单日期`,'%Y-%m') <= ?
  and `保险公司` = ?
  and `省` = ?
  and `8-险种名称` in ('交强险','商业险')
order by sum_commission desc,
         gross_profit
limit 100
'''

    @Override
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
            return baseSql.rows(getQuernCommissionUp().replace("#", type), [commission, commission, row.'order_month', row.'保险公司', row.'省'])
        }

        if (commission < 0) {
            return baseSql.rows(getQuernCommissionDown().replace("#", type), [0 - commission, row.'order_month', row.'保险公司', row.'省'])
        }
    }

}
