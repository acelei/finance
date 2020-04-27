package com.cheche365.service


import org.springframework.stereotype.Service

@Service
class MatchResultSideData extends MatchSideData {
    String querySettlement = "select id,s_id,sum_fee as fee,sum_commission as commission,`14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,`42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,DATE_FORMAT(`9-保单出单日期`,'%Y-%m') as order_month,`保险公司`,`省`, null as flag from result_#_2 where `8-险种名称` in ('交强险','商业险') and c_id is null and handle_sign=6 and date_format(`9-保单出单日期`,'%Y')='2019'"
    String queryCommission = "select id,c_id,sum_fee as fee,sum_commission as commission,`14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,`42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,DATE_FORMAT(`9-保单出单日期`,'%Y-%m') as order_month,`保险公司`,`省`,`40-代理人名称`, null as flag from result_#_2 where `8-险种名称` in ('交强险','商业险') and s_id is null and handle_sign=6 and date_format(`9-保单出单日期`,'%Y')='2019'"
    String sTable = "result_#_2"
    String cTable = "result_#_2"
}
