package com.cheche365.service

import com.cheche365.util.ExcelUtil2
import groovy.util.logging.Slf4j
import org.apache.commons.io.FileUtils
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
         left join business_replace_ref b on a.id = b.finance_id and b.table_name = 'settlement_#'
         left join result_gross_margin_ref c on a.id = c.s_id and c.type in (2,3)  and c.table_name in ('result_#_2', 'settlement_#', 'commission_#')
         left join commission_# d on d.id = c.c_id and c.type = 2
         left join settlement_# e on e.id = c.c_id and c.type = 3
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
         left join commission_# e on e.id = c.s_id and c.type = 4
'''
    String cleanSql = "truncate result_#_final"
    String insertSql = "insert into result_#_final "
    String clean2Sql = 'truncate result_#_2_final'
    String result2Sql = '''
insert into result_#_2_final (s_id,
                               c_id,
                               source_file,
                               `2-保代机构`,
                               `3-出单保险代理机构（车车科技适用）`,
                               `4-发票付款方（与发票一致）`,
                               `5-投保人名称`,
                               `6-保单单号`,
                               `7-出单保险公司（明细至保险公司分支机构）`,
                               `8-险种名称`,
                               `9-保单出单日期`,
                               `10-全保费`,
                               `11-净保费`,
                               `12-手续费等级（对应点位台账）`,
                               `14-手续费总额（报行内+报行外）(含税)`,
                               `15-手续费总额（报行内+报行外）(不含税)`,
                               `16-收入入账月度`,
                               `17-凭证号`,
                               `19-手续费金额（含税）`,
                               `20-手续费金额（不含税）`,
                               `21-回款月度`,
                               `22-凭证号`,
                               `23-收款金额`,
                               `24-开票单位`,
                               `25-开票日期`,
                               `27-开票金额（不含税）`,
                               `28-开票金额（含税）`,
                               `29-20191231应收账款（含已开票和未开票）`,
                               `30-开票日期`,
                               `31-应收回款月度`,
                               `32-收款凭证号`,
                               `33-收款金额`,
                               `34-开票单位`,
                               `35-开票日期`,
                               `36-开票金额（不含税）`,
                               `37-开票金额（含税）`,
                               `38-尚未开票金额（不含税）`,
                               `39-尚未开票金额（含税）`,
                               `40-代理人名称`,
                               `42-佣金金额（已入账）`,
                               `43-支付主体`,
                               `45-支付金额`,
                               `46-未计提佣金（19年底尚未入帐）`,
                               `保险公司`,
                               `保险公司id`,
                               `省`,
                               `市`,
                               sum_fee,
                               sum_commission,
                               gross_profit)
select group_concat(s_id)                                  as s_id,
       group_concat(c_id)                                  as c_id,
       source_file,
       `2-保代机构`,
       `3-出单保险代理机构（车车科技适用）`,
       `4-发票付款方（与发票一致）`,
       `5-投保人名称`,
       `6-保单单号`,
       `7-出单保险公司（明细至保险公司分支机构）`,
       `8-险种名称`,
       `9-保单出单日期`,
       if(max(0 + `10-全保费`) > 0,
          max(0 + `10-全保费`) + if(min(0 + `10-全保费`) < 0, min(0 + `10-全保费`), 0),
          min(0 + `10-全保费`))                               as `10-全保费`,
       if(max(0 + `11-净保费`) > 0,
          max(0 + `11-净保费`) + if(min(0 + `11-净保费`) < 0, min(0 + `11-净保费`), 0),
          min(0 + `11-净保费`))                               as `11-净保费`,
       max(`12-手续费等级（对应点位台账）`)                             as `12-手续费等级（对应点位台账）`,
       sum(`14-手续费总额（报行内+报行外）(含税)`)                        as `14-手续费总额（报行内+报行外）(含税)`,
       sum(`15-手续费总额（报行内+报行外）(不含税)`)                       as `15-手续费总额（报行内+报行外）(不含税)`,
       max(`16-收入入账月度`)                                    as `16-收入入账月度`,
       max(`17-凭证号`)                                       as `17-凭证号`,
       sum(`19-手续费金额（含税）`)                                 as `19-手续费金额（含税）`,
       sum(`20-手续费金额（不含税）`)                                as `20-手续费金额（不含税）`,
       max(`21-回款月度`)                                      as `21-回款月度`,
       max(`22-凭证号`)                                       as `22-凭证号`,
       sum(`23-收款金额`)                                      as `23-收款金额`,
       max(`24-开票单位`)                                      as `24-开票单位`,
       max(`25-开票日期`)                                      as `25-开票日期`,
       sum(`27-开票金额（不含税）`)                                 as `27-开票金额（不含税）`,
       sum(`28-开票金额（含税）`)                                  as `28-开票金额（含税）`,
       sum(`29-20191231应收账款（含已开票和未开票）`)                    as `29-20191231应收账款（含已开票和未开票）`,
       max(`30-开票日期`)                                      as `30-开票日期`,
       max(`31-应收回款月度`)                                    as `31-应收回款月度`,
       max(`32-收款凭证号`)                                     as `32-收款凭证号`,
       sum(`33-收款金额`)                                      as `33-收款金额`,
       max(`34-开票单位`)                                      as `34-开票单位`,
       max(`35-开票日期`)                                      as `35-开票日期`,
       sum(`36-开票金额（不含税）`)                                 as `36-开票金额（不含税）`,
       sum(`37-开票金额（含税）`)                                  as `37-开票金额（含税）`,
       sum(`38-尚未开票金额（不含税）`)                               as `38-尚未开票金额（不含税）`,
       sum(`39-尚未开票金额（含税）`)                                as `39-尚未开票金额（含税）`,
       max(`40-代理人名称`)                                     as `40-代理人名称`,
       sum(`42-佣金金额（已入账）`)                                 as `42-佣金金额（已入账）`,
       max(`43-支付主体`)                                      as `43-支付主体`,
       sum(`45-支付金额`)                                      as `45-支付金额`,
       sum(`46-未计提佣金（19年底尚未入帐）`)                           as `46-未计提佣金（19年底尚未入帐）`,
       `保险公司`,
       `保险公司id`,
       `省`,
       `市`,
       sum(sum_fee)                                        as sum_fee,
       sum(sum_commission)                                 as sum_commission,
       (sum(sum_fee) - sum(sum_commission)) / sum(sum_fee) as gross_profit
from (select a.id                                                                                     as `s_id`,
             null                                                                                     as `c_id`,
             a.source_file,
             a.`2-保代机构`,
             a.`3-出单保险代理机构（车车科技适用）`,
             a.`4-发票付款方（与发票一致）`,
             if(b.finance_id is not null, b.applicant,
                if(d.id is not null, d.`5-投保人名称`, if(e.id is not null, e.`5-投保人名称`, a.`5-投保人名称`)))    as '5-投保人名称',
             if(b.finance_id is not null, b.policy_no,
                if(d.id is not null, d.`6-保单单号`, if(e.id is not null, e.`6-保单单号`, a.`6-保单单号`)))       as '6-保单单号',
             a.`7-出单保险公司（明细至保险公司分支机构）`,
             if(b.finance_id is not null, if(b.insurance_type is not null, b.insurance_type, if(b.insurance_type_id = 1, '交强险', '商业险')),
                if(d.id is not null, d.`8-险种名称`, if(e.id is not null, e.`8-险种名称`, a.`8-险种名称`)))       as '8-险种名称',
             if(b.finance_id is not null, b.order_date,
                if(d.id is not null, d.`9-保单出单日期`, if(e.id is not null, e.`9-保单出单日期`, a.`9-保单出单日期`))) as '9-保单出单日期',
             if(b.finance_id is not null, b.premium,
                if(d.id is not null, d.`10-全保费`, if(e.id is not null, e.`10-全保费`, a.`10-全保费`)))       as '10-全保费',
             if(b.finance_id is not null, b.premium / 1.06,
                if(d.id is not null, d.`11-净保费`, if(e.id is not null, e.`11-净保费`, a.`11-净保费`)))       as '11-净保费',
             a.`12-手续费等级（对应点位台账）`,
             ifnull(a.`13-手续费率`, 0)                                                                   as `13-手续费率`,
             a.`14-手续费总额（报行内+报行外）(含税)`,
             a.`15-手续费总额（报行内+报行外）(不含税)`,
             a.`16-收入入账月度`,
             a.`17-凭证号`,
             ifnull(a.`18-手续费比例`, 0)                                                                  as `18-手续费比例`,
             a.`19-手续费金额（含税）`,
             a.`20-手续费金额（不含税）`,
             a.`21-回款月度`,
             a.`22-凭证号`,
             a.`23-收款金额`,
             a.`24-开票单位`,
             a.`25-开票日期`,
             ifnull(a.`26-手续费比例`, 0)                                                                  as `26-手续费比例`,
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
             null                                                                                     as `40-代理人名称`,
             null                                                                                     as `41-佣金比例`,
             0                                                                                        as `42-佣金金额（已入账）`,
             null                                                                                     as `43-支付主体`,
             null                                                                                     as `44-支付比例`,
             0                                                                                        as `45-支付金额`,
             0                                                                                        as `46-未计提佣金（19年底尚未入帐）`,
             a.`保险公司`,
             a.`省`,
             a.`市`,
             a.`保险公司id`,
             a.sum_fee,
             0                                                                                        as sum_commission
      from settlement_# a
               left join business_replace_ref b on a.id = b.finance_id and b.table_name = 'settlement_#'
               left join result_gross_margin_ref c on a.id = c.s_id and c.type in (2, 3) and
                                                      c.table_name in ('result_#_2', 'settlement_#', 'commission_#')
               left join commission_# d on d.id = c.c_id and c.type = 2
               left join settlement_# e on e.id = c.c_id and c.type = 3
      union all
      select null                                                                                     as `s_id`,
             a.id                                                                                     as `c_id`,
             a.source_file,
             a.`2-保代机构`,
             a.`3-出单保险代理机构（车车科技适用）`,
             a.`4-发票付款方（与发票一致）`,
             if(b.finance_id is not null, b.applicant,
                if(d.id is not null, d.`5-投保人名称`, if(e.id is not null, e.`5-投保人名称`, a.`5-投保人名称`)))    as '5-投保人名称',
             if(b.finance_id is not null, b.policy_no,
                if(d.id is not null, d.`6-保单单号`, if(e.id is not null, e.`6-保单单号`, a.`6-保单单号`)))       as '6-保单单号',
             a.`7-出单保险公司（明细至保险公司分支机构）`,
             if(b.finance_id is not null, if(b.insurance_type is not null, b.insurance_type, if(b.insurance_type_id = 1, '交强险', '商业险')),
                if(d.id is not null, d.`8-险种名称`, if(e.id is not null, e.`8-险种名称`, a.`8-险种名称`)))       as '8-险种名称',
             if(b.finance_id is not null, b.order_date,
                if(d.id is not null, d.`9-保单出单日期`, if(e.id is not null, e.`9-保单出单日期`, a.`9-保单出单日期`))) as '9-保单出单日期',
             if(b.finance_id is not null, b.premium,
                if(d.id is not null, d.`10-全保费`, if(e.id is not null, e.`10-全保费`, a.`10-全保费`)))       as '10-全保费',
             if(b.finance_id is not null, b.premium / 1.06,
                if(d.id is not null, d.`11-净保费`, if(e.id is not null, e.`11-净保费`, a.`11-净保费`)))       as '11-净保费',
             null                                                                                     as `12-手续费等级（对应点位台账）`,
             null                                                                                     as `13-手续费率`,
             0                                                                                        as `14-手续费总额（报行内+报行外）(含税)`,
             0                                                                                        as `15-手续费总额（报行内+报行外）(不含税)`,
             null                                                                                     as `16-收入入账月度`,
             null                                                                                     as `17-凭证号`,
             null                                                                                     as `18-手续费比例`,
             0                                                                                        as `19-手续费金额（含税）`,
             0                                                                                        as `20-手续费金额（不含税）`,
             null                                                                                     as `21-回款月度`,
             null                                                                                     as `22-凭证号`,
             0                                                                                        as `23-收款金额`,
             null                                                                                     as `24-开票单位`,
             null                                                                                     as `25-开票日期`,
             null                                                                                     as `26-手续费比例`,
             0                                                                                        as `27-开票金额（不含税）`,
             0                                                                                        as `28-开票金额（含税）`,
             0                                                                                        as `29-20191231应收账款（含已开票和未开票）`,
             null                                                                                     as `30-开票日期`,
             null                                                                                     as `31-应收回款月度`,
             null                                                                                     as `32-收款凭证号`,
             0                                                                                        as `33-收款金额`,
             null                                                                                     as `34-开票单位`,
             null                                                                                     as `35-开票日期`,
             0                                                                                        as `36-开票金额（不含税）`,
             0                                                                                        as `37-开票金额（含税）`,
             0                                                                                        as `38-尚未开票金额（不含税）`,
             0                                                                                        as `39-尚未开票金额（含税）`,
             a.`40-代理人名称`,
             ifnull(a.`41-佣金比例`, 0)                                                                   as `41-佣金比例`,
             a.`42-佣金金额（已入账）`,
             a.`43-支付主体`,
             ifnull(a.`44-支付比例`, 0)                                                                   as `44-支付比例`,
             a.`45-支付金额`,
             a.`46-未计提佣金（19年底尚未入帐）`,
             a.`保险公司`,
             a.`省`,
             a.`市`,
             a.`保险公司id`,
             0                                                                                        as sum_fee,
             a.sum_commission
      from commission_# a
               left join business_replace_ref b on a.id = b.finance_id and b.table_name = 'commission_#'
               left join result_gross_margin_ref c on a.id = c.c_id and c.type in (1, 4) and
                                                      c.table_name in ('result_#_2', 'settlement_#', 'commission_#')
               left join settlement_# d on d.id = c.s_id and c.type = 1
               left join commission_# e on e.id = c.s_id and c.type = 4) t
group by `6-保单单号`, if(`8-险种名称` in ('交强险', '商业险'), `8-险种名称`, 'ODS')
'''

    void run(String type) {
        log.info("输出final result:{}", type)
//        result(type)
        result2(type)
        initData.fixPremium(type, "_final")
        fix(type)
        log.info("输出final result完成:{}", type)
    }

    String fixSql = '''
update result_#_2_final set `13-手续费率`=`14-手续费总额（报行内+报行外）(含税)`/`11-净保费`,
                             `15-手续费总额（报行内+报行外）(不含税)`=`14-手续费总额（报行内+报行外）(含税)`/1.06,
                             `18-手续费比例`=`19-手续费金额（含税）`/`11-净保费`,
                             `20-手续费金额（不含税）`=`19-手续费金额（含税）`/1.06,
                             `26-手续费比例`=`28-开票金额（含税）`/`11-净保费`,
                             `27-开票金额（不含税）`=`28-开票金额（含税）`/1.06,
                             `36-开票金额（不含税）`=`37-开票金额（含税）`/1.06,
                             `38-尚未开票金额（不含税）`=`39-尚未开票金额（含税）`/1.06,
                             `41-佣金比例`=`42-佣金金额（已入账）`/`11-净保费`,
                             `44-支付比例`=`45-支付金额`/`11-净保费`
'''

    String fixSql2 = '''
update result_#_2_final set `13-手续费率`=`14-手续费总额（报行内+报行外）(含税)`/`11-净保费`,
                             `15-手续费总额（报行内+报行外）(不含税)`=`14-手续费总额（报行内+报行外）(含税)`/1.06,
                             `18-手续费比例`=`19-手续费金额（含税）`/`11-净保费`,
                             `26-手续费比例`=`28-开票金额（含税）`/`11-净保费`,
                             `27-开票金额（不含税）`=`28-开票金额（含税）`/1.06,
                             `36-开票金额（不含税）`=`37-开票金额（含税）`/1.06,
                             `38-尚未开票金额（不含税）`=`39-尚未开票金额（含税）`/1.06,
                             `41-佣金比例`=`42-佣金金额（已入账）`/`11-净保费`,
                             `44-支付比例`=`45-支付金额`/`11-净保费`
'''
    List<String> zTypes = ["henan", "sichuan", "fujian", "foshan"]

    void fix(String type) {
        if (zTypes.contains(type)) {
            baseSql.executeUpdate(fixSql2.replace("#", type))
        }else {
            baseSql.executeUpdate(fixSql.replace("#", type))
        }
    }

    String queryResult = "select * from result_#_2_final"
    String queryErrorSettlement = "select * from settlement_# where handle_sign=6"
    String queryErrorCommission = "select * from commission_# where handle_sign=6"
//    List<String> head = ["id", "s_id", "d_id", "c_id", "source_file","1-序号", "2-保代机构", "3-出单保险代理机构（车车科技适用）", "4-发票付款方（与发票一致）", "5-投保人名称", "6-保单单号", "7-出单保险公司（明细至保险公司分支机构）", "8-险种名称", "9-保单出单日期", "10-全保费", "11-净保费", "12-手续费等级（对应点位台账）", "13-手续费率", "14-手续费总额（报行内+报行外）(含税)", "15-手续费总额（报行内+报行外）(不含税)", "16-收入入账月度", "17-凭证号", "18-手续费比例", "19-手续费金额（含税）", "20-手续费金额（不含税）", "21-回款月度", "22-凭证号", "23-收款金额", "24-开票单位", "25-开票日期", "26-手续费比例", "27-开票金额（不含税）", "28-开票金额（含税）", "29-20191231应收账款（含已开票和未开票）", "30-开票日期", "31-应收回款月度", "32-收款凭证号", "33-收款金额", "34-开票单位", "35-开票日期", "36-开票金额（不含税）", "37-开票金额（含税）", "38-尚未开票金额（不含税）", "39-尚未开票金额（含税）", "40-代理人名称", "41-佣金比例", "42-佣金金额（已入账）", "43-支付主体", "44-支付比例", "45-支付金额", "46-未计提佣金（19年底尚未入帐）"]
    List<String> head = ["1-序号", "2-保代机构", "3-出单保险代理机构（车车科技适用）", "4-发票付款方（与发票一致）", "6-保单单号", "7-出单保险公司（明细至保险公司分支机构）", "8-险种名称", "9-保单出单日期", "10-全保费", "11-净保费", "12-手续费等级（对应点位台账）", "13-手续费率", "14-手续费总额（报行内+报行外）(含税)", "15-手续费总额（报行内+报行外）(不含税)", "16-收入入账月度", "17-凭证号", "18-手续费比例", "19-手续费金额（含税）", "20-手续费金额（不含税）", "21-回款月度", "22-凭证号", "23-收款金额", "24-开票单位", "25-开票日期", "26-手续费比例", "27-开票金额（不含税）", "28-开票金额（含税）", "29-20191231应收账款（含已开票和未开票）", "30-开票日期", "31-应收回款月度", "32-收款凭证号", "33-收款金额", "34-开票单位", "35-开票日期", "36-开票金额（不含税）", "37-开票金额（含税）", "38-尚未开票金额（不含税）", "39-尚未开票金额（含税）", "40-代理人名称", "41-佣金比例", "42-佣金金额（已入账）", "43-支付主体", "44-支付比例", "45-支付金额", "46-未计提佣金（19年底尚未入帐）"]

    File exportResult(String type) {
        return exportResult(type, null)
    }

    File exportResult(String type, File targetFile) {
        List<Map> rows
//        File f = null
//        List<File> fileList = new ArrayList<>()
        rows = baseSql.rows(queryResult.replace("#", type))
        if (rows.size() > 0) {
            log.info("导出整合数据:{}", type)
//            f = File.createTempFile("审计台账_#".replace("#", type), '.xlsx', ExcelUtil2.tmp)
            targetFile.deleteOnExit()
            ExcelUtil2.writeToExcel(head, rows).renameTo(targetFile)
//            fileList.add f
        }

//        rows = baseSql.rows(queryErrorSettlement.replace("#", type))
//        if (rows.size() > 0) {
//            log.info("导出剩余结算数据:{}", type)
//            File f = File.createTempFile("结算_#".replace("#", type), '.xlsx', ExcelUtil2.tmp)
//            ExcelUtil2.writeToExcel(head, rows).renameTo(f)
//            fileList.add f
//        }
//
//
//        rows = baseSql.rows(queryErrorCommission.replace("#", type))
//        if (rows.size() > 0) {
//            log.info("导出剩余付佣数据:{}", type)
//            File f = File.createTempFile("佣金_#".replace("#", type), '.xlsx', ExcelUtil2.tmp)
//            ExcelUtil2.writeToExcel(head, rows).renameTo(f)
//            fileList.add f
//        }
//
//        return ExcelUtil2.zipFiles(fileList, targetFile)
        return targetFile
    }
}
