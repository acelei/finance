package com.cheche365.service

import org.springframework.stereotype.Service

@Service("reMatchResultSideData")
class ReMatchResultSideData extends ReMatchSideData {
    String errorSettlementSide = "select id,s_id,sum_fee as fee,sum_commission as commission,`14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,`42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,DATE_FORMAT(`9-保单出单日期`,'%Y-%m') as order_month,`保险公司`,`省` from result_#_2 where handle_sign=6 and c_id is null"
    String errorCommissionSide = "select id,c_id,sum_fee as fee,sum_commission as commission,`14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,`42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,DATE_FORMAT(`9-保单出单日期`,'%Y-%m') as order_month,`保险公司`,`省`,`40-代理人名称` from result_#_2 where handle_sign=6 and s_id is null"

    String sTable = "result_#_2"
    String cTable = "result_#_2"
}
