package com.cheche365.service

import groovy.sql.GroovyRowResult
import groovy.util.logging.Slf4j
import org.springframework.stereotype.Service

@Service("reMatchSPSideData4")
@Slf4j
class ReMatchSPSideData4 extends ReMatchSideData {
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
  and DATE_FORMAT(`9-保单出单日期`,'%Y-%m') <= ?
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
  and DATE_FORMAT(`9-保单出单日期`,'%Y-%m') <= ?
  and `省` = ?
  and `8-险种名称` in ('交强险','商业险')
order by rand()
limit 100
'''
    @Override
    void run(String type) {
        log.info("反向匹配佣金单边数据:{}-{}", getcTable(), type)
        commissionMatch(type)
        log.info("反向匹配佣金单边数据完成:{}-{}", getcTable(), type)
    }

    @Override
    List<GroovyRowResult> findResult(GroovyRowResult row, String type) {
        def commission = row.commission as double

        if (commission > 0) {
            return baseSql.rows(getQuernCommissionUp().replace("#", type), [commission, commission, row.'order_month', row.'省'])
        }

        if (commission < 0) {
            return baseSql.rows(getQuernCommissionDown().replace("#", type), [0 - commission, row.'order_month', row.'省'])
        }
    }

}
