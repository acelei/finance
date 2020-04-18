package com.cheche365.service

import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
@Slf4j
class ResultService extends DataCombine {
    @Autowired
    private InitData initData

    String settlementQuery = '''
select a.`id`,
       a.`s_id`,
       a.`d_id`,
       a.`c_id`,
       a.`source_file`,
       a.`1-序号`,
       a.`2-保代机构`,
       a.`3-出单保险代理机构（车车科技适用）`,
       a.`4-发票付款方（与发票一致）`,
       if(b.finance_id is not null, b.applicant, if(d.id is not null, d.`5-投保人名称`, if(e.id is not null, e.`5-投保人名称`, a.`5-投保人名称`)))    as '5-投保人名称',
       if(b.finance_id is not null, b.policy_no, if(d.id is not null, d.`6-保单单号`, if(e.id is not null, e.`6-保单单号`, a.`6-保单单号`)))      as '6-保单单号',
       a.`7-出单保险公司（明细至保险公司分支机构）`,
       if(b.finance_id is not null, if(b.insurance_type_id = 1, '交强险', '商业险'),
          if(d.id is not null, d.`8-险种名称`, if(e.id is not null, e.`8-险种名称`, a.`8-险种名称`)))                                             as '8-险种名称',
       if(b.finance_id is not null, b.order_date, if(d.id is not null, d.`9-保单出单日期`, if(e.id is not null, e.`9-保单出单日期`, a.`9-保单出单日期`))) as '9-保单出单日期',
       if(b.finance_id is not null, b.premium, if(d.id is not null, d.`10-全保费`, if(e.id is not null, e.`10-全保费`, a.`10-全保费`)))        as '10-全保费',
       if(b.finance_id is not null, b.premium / 1.06, if(d.id is not null, d.`11-净保费`, if(e.id is not null, e.`11-净保费`, a.`11-净保费`))) as '11-净保费',
       a.`12-手续费等级（对应点位台账）`,
       a.`13-手续费率`,
       a.`14-手续费总额（报行内+报行外）(含税)`,
       a.`15-手续费总额（报行内+报行外）(不含税)`,
       a.`16-收入入账月度`,
       a.`17-凭证号`,
       a.`18-手续费比例`,
       a.`19-手续费金额（含税）`,
       a.`20-手续费金额（不含税）`,
       a.`21-回款月度`,
       a.`22-凭证号`,
       a.`23-收款金额`,
       a.`24-开票单位`,
       a.`25-开票日期`,
       a.`26-手续费比例`,
       a.`27-开票金额（不含税）`,
       a.`28-开票金额（含税）`,
       a.`29-20191231应收账款（含已开票和未开票）`,
       a.`30-开票日期`,
       a.`31-应收回款月度`,
       a.`32-收款凭证号`,
       a.`33-收款金额`,
       a.`34-开票单位`,
       a.`35-开票日期`,
       a.`36-开票金额（不含税）`,
       a.`37-开票金额（含税）`,
       a.`38-尚未开票金额（不含税）`,
       a.`39-尚未开票金额（含税）`,
       a.`40-代理人名称`,
       a.`41-佣金比例`,
       a.`42-佣金金额（已入账）`,
       a.`43-支付主体`,
       a.`44-支付比例`,
       a.`45-支付金额`,
       a.`46-未计提佣金（19年底尚未入帐）`,
       a.`保险公司`,
       a.`省`,
       a.`市`,
       a.`保险公司id`,
       a.sum_fee,
       a.sum_commission
from settlement_# a
         left join business_replace_ref b on a.id = b.finance_id and b.table_name = 'commission_#'
         left join result_gross_margin_ref c on a.id = c.s_id and c.type in (2,3)  and c.table_name in ('result_#_2', 'settlement_#', 'commission_#')
         left join commission_# d on d.id = c.c_id and c.type = 2
         left join settlement_# e on d.id = c.c_id and c.type = 3
         where a.handle_sign!=6
'''
    String commissionQuery = '''
select a.`id`,
       a.`s_id`,
       a.`d_id`,
       a.`c_id`,
       a.`source_file`,
       a.`1-序号`,
       a.`2-保代机构`,
       a.`3-出单保险代理机构（车车科技适用）`,
       a.`4-发票付款方（与发票一致）`,
       if(b.finance_id is not null, b.applicant, if(d.id is not null, d.`5-投保人名称`, if(e.id is not null, e.`5-投保人名称`, a.`5-投保人名称`)))    as '5-投保人名称',
       if(b.finance_id is not null, b.policy_no, if(d.id is not null, d.`6-保单单号`, if(e.id is not null, e.`6-保单单号`, a.`6-保单单号`)))      as '6-保单单号',
       a.`7-出单保险公司（明细至保险公司分支机构）`,
       if(b.finance_id is not null, if(b.insurance_type_id = 1, '交强险', '商业险'),
          if(d.id is not null, d.`8-险种名称`, if(e.id is not null, e.`8-险种名称`, a.`8-险种名称`)))                                             as '8-险种名称',
       if(b.finance_id is not null, b.order_date, if(d.id is not null, d.`9-保单出单日期`, if(e.id is not null, e.`9-保单出单日期`, a.`9-保单出单日期`))) as '9-保单出单日期',
       if(b.finance_id is not null, b.premium, if(d.id is not null, d.`10-全保费`, if(e.id is not null, e.`10-全保费`, a.`10-全保费`)))        as '10-全保费',
       if(b.finance_id is not null, b.premium / 1.06, if(d.id is not null, d.`11-净保费`, if(e.id is not null, e.`11-净保费`, a.`11-净保费`))) as '11-净保费',
       a.`12-手续费等级（对应点位台账）`,
       a.`13-手续费率`,
       a.`14-手续费总额（报行内+报行外）(含税)`,
       a.`15-手续费总额（报行内+报行外）(不含税)`,
       a.`16-收入入账月度`,
       a.`17-凭证号`,
       a.`18-手续费比例`,
       a.`19-手续费金额（含税）`,
       a.`20-手续费金额（不含税）`,
       a.`21-回款月度`,
       a.`22-凭证号`,
       a.`23-收款金额`,
       a.`24-开票单位`,
       a.`25-开票日期`,
       a.`26-手续费比例`,
       a.`27-开票金额（不含税）`,
       a.`28-开票金额（含税）`,
       a.`29-20191231应收账款（含已开票和未开票）`,
       a.`30-开票日期`,
       a.`31-应收回款月度`,
       a.`32-收款凭证号`,
       a.`33-收款金额`,
       a.`34-开票单位`,
       a.`35-开票日期`,
       a.`36-开票金额（不含税）`,
       a.`37-开票金额（含税）`,
       a.`38-尚未开票金额（不含税）`,
       a.`39-尚未开票金额（含税）`,
       a.`40-代理人名称`,
       a.`41-佣金比例`,
       a.`42-佣金金额（已入账）`,
       a.`43-支付主体`,
       a.`44-支付比例`,
       a.`45-支付金额`,
       a.`46-未计提佣金（19年底尚未入帐）`,
       a.`保险公司`,
       a.`省`,
       a.`市`,
       a.`保险公司id`,
       a.sum_fee,
       a.sum_commission
from commission_# a
         left join business_replace_ref b on a.id = b.finance_id and b.table_name = 'commission_#'
         left join result_gross_margin_ref c on a.id = c.c_id and c.type in (1,4) and c.table_name in ('result_#_2', 'settlement_#', 'commission_#')
         left join settlement_# d on d.id = c.s_id and c.type = 1
         left join commission_# e on d.id = c.s_id and c.type = 4
         where a.handle_sign!=6
'''
    String cleanSql = "truncate result_#_final"
    String insertSql = "insert into result_#_final "
    String clean2Sql = 'truncate result_#_2_final'
    String result2Sql = '''
insert into result_#_2_final (d_id,s_id,c_id,`2-保代机构`,`3-出单保险代理机构（车车科技适用）`,`4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`12-手续费等级（对应点位台账）`,`13-手续费率`,
`14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,`19-手续费金额（含税）`,`20-手续费金额（不含税）`,`23-收款金额`,`27-开票金额（不含税）`,`28-开票金额（含税）`,`29-20191231应收账款（含已开票和未开票）`,`33-收款金额`,`36-开票金额（不含税）`,`37-开票金额（含税）`,`38-尚未开票金额（不含税）`,`39-尚未开票金额（含税）`,
`40-代理人名称`,`42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`, `保险公司`,`保险公司id`,`省`,`市`,sum_fee,sum_commission,gross_profit)
select group_concat(id) d_id,group_concat(s_id) as s_id,group_concat(c_id) as c_id,
       `2-保代机构`,
       `3-出单保险代理机构（车车科技适用）`,
       `4-发票付款方（与发票一致）`,
       `5-投保人名称`,
       `6-保单单号`,
       `7-出单保险公司（明细至保险公司分支机构）`,
       `8-险种名称`,
       `9-保单出单日期`,
       if(max(0+ifnull(`10-全保费`,0))>0,max(0+ifnull(`10-全保费`,0))-if(min(0+ifnull(`10-全保费`,0))<0,min(0+ifnull(`10-全保费`,0)),0),min(0+ifnull(`10-全保费`,0))) as `10-全保费`,
       if(max(0+ifnull(`11-净保费`,0))>0,max(0+ifnull(`11-净保费`,0))-if(min(0+ifnull(`11-净保费`,0))<0,min(0+ifnull(`11-净保费`,0)),0),min(0+ifnull(`11-净保费`,0))) as `11-净保费`,
       `12-手续费等级（对应点位台账）`,
       `13-手续费率`,
       sum(if(`14-手续费总额（报行内+报行外）(含税)` is not null and s_id is not null,`14-手续费总额（报行内+报行外）(含税)`, 0)) as `14-手续费总额（报行内+报行外）(含税)`,
       sum(if(`15-手续费总额（报行内+报行外）(不含税)` is not null and s_id is not null,`15-手续费总额（报行内+报行外）(不含税)`, 0)) as `15-手续费总额（报行内+报行外）(不含税)`,
       sum(if(`19-手续费金额（含税）` is not null and s_id is not null,`19-手续费金额（含税）`, 0)) as `19-手续费金额（含税）`,
       sum(if(`20-手续费金额（不含税）` is not null and s_id is not null,`20-手续费金额（不含税）`, 0)) as `20-手续费金额（不含税）`,
       sum(if(`23-收款金额` is not null and s_id is not null,`23-收款金额`, 0)) as `23-收款金额`,
       sum(if(`27-开票金额（不含税）` is not null and s_id is not null,`27-开票金额（不含税）`, 0)) as `27-开票金额（不含税）`,
       sum(if(`28-开票金额（含税）` is not null and s_id is not null,`28-开票金额（含税）`, 0)) as `28-开票金额（含税）`,
       sum(if(`29-20191231应收账款（含已开票和未开票）` is not null and s_id is not null,`29-20191231应收账款（含已开票和未开票）`, 0)) as `29-20191231应收账款（含已开票和未开票）`,
       sum(if(`33-收款金额` is not null and s_id is not null,`33-收款金额`, 0)) as `33-收款金额`,
       sum(if(`36-开票金额（不含税）` is not null and s_id is not null,`36-开票金额（不含税）`, 0)) as `36-开票金额（不含税）`,
       sum(if(`37-开票金额（含税）` is not null and s_id is not null,`37-开票金额（含税）`, 0)) as `37-开票金额（含税）`,
       sum(if(`38-尚未开票金额（不含税）` is not null and s_id is not null,`38-尚未开票金额（不含税）`, 0)) as `38-尚未开票金额（不含税）`,
       sum(if(`39-尚未开票金额（含税）` is not null and s_id is not null,`39-尚未开票金额（含税）`, 0)) as `39-尚未开票金额（含税）`,
       `40-代理人名称`,
       sum(if(`42-佣金金额（已入账）` is not null and c_id is not null,`42-佣金金额（已入账）`,0)) as `42-佣金金额（已入账）`,
       sum(if(`45-支付金额` is not null and c_id is not null,`45-支付金额`,0)) as `45-支付金额`,
       sum(if(`46-未计提佣金（19年底尚未入帐）` is not null and c_id is not null,`46-未计提佣金（19年底尚未入帐）`,0)) as `46-未计提佣金（19年底尚未入帐）`,
       `保险公司`,`保险公司id`,`省`,`市`,
       sum(sum_fee) as sum_fee,
       sum(sum_commission) as sum_commission,
       (sum(sum_fee)-sum(sum_commission))/sum(sum_fee) as gross_profit
from result_#_final group by  `6-保单单号`, if(`8-险种名称` in ('交强险','商业险'),`8-险种名称`,'ODS')'''

    void run(final String type) {
        result(type)
        result2(type)
        initData.fixPremium(type, "_final")
    }
}
