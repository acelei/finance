package com.cheche365.service

import groovy.sql.GroovyRowResult
import groovy.util.logging.Slf4j
import org.springframework.stereotype.Service

/**
 * 特殊调整
 */
@Service("reMatchSPSideData5")
@Slf4j
class ReMatchSPSideData5 extends ReMatchSideData {

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
  and abs(0+`11-净保费`)-sum_fee > ?
  and 0 - sum_fee < ?
  and DATE_FORMAT(`9-保单出单日期`,'%Y-%m')  = '2019-09'
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
  and 0+sum_fee > ?
  and DATE_FORMAT(`9-保单出单日期`,'%Y-%m')  = '2019-09'
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
  and abs(0+`11-净保费`)-sum_commission > ?
  and 0-sum_commission < ?
  and DATE_FORMAT(`9-保单出单日期`,'%Y-%m')  = '2019-09'
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
  and DATE_FORMAT(`9-保单出单日期`,'%Y-%m') = '2019-09'
  and `保险公司` = ?
  and `省` = ?
  and `8-险种名称` in ('交强险','商业险')
order by rand()
limit 100
'''

    @Override
    List<GroovyRowResult> findResult(GroovyRowResult row, String type) {
        def fee = row.fee as double
        def commission = row.commission as double
        if (fee > 0) {
            return baseSql.rows(getQuernSettlementUp().replace("#", type), [fee, fee, row.'保险公司', row.'省' , row.'4-发票付款方（与发票一致）'])
        }

        if (fee < 0) {
            return baseSql.rows(getQuernSettlementDown().replace("#", type), [0 - fee, row.'保险公司', row.'省', row.'4-发票付款方（与发票一致）'])
        }

        if (commission > 0) {
            return baseSql.rows(getQuernCommissionUp().replace("#", type), [commission, commission, row.'保险公司', row.'省'])
        }

        if (commission < 0) {
            return baseSql.rows(getQuernCommissionDown().replace("#", type), [0 - commission, row.'保险公司', row.'省'])
        }
    }

}
