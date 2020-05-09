package com.cheche365.service


import groovy.util.logging.Slf4j
import org.springframework.stereotype.Service

@Service
@Slf4j
class ResultService3 extends ResultService {
    String tableFinal = 'result_#_3_final'
    String clean2Sql = 'truncate result_#_3_final'
    String result2Sql = '''
insert into result_#_3_final (s_id,
                               c_id,
                               source_file,
                               `1-序号`,
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
       @rownum:=@rownum+1 AS `1-序号`,
       `2-保代机构`,
       `3-出单保险代理机构（车车科技适用）`,
       `4-发票付款方（与发票一致）`,
       `5-投保人名称`,
       `6-保单单号`,
       `7-出单保险公司（明细至保险公司分支机构）`,
       `8-险种名称`,
       `9-保单出单日期`,
       if(abs(sum(sum_fee))>10 or abs(sum(sum_commission))>10, max(0 + `10-全保费`),
       if(max(0 + `10-全保费`) > 0,
          max(0 + `10-全保费`) + if(min(0 + `10-全保费`) < 0, min(0 + `10-全保费`), 0),
          min(0 + `10-全保费`)))            as `10-全保费`,
       if(abs(sum(sum_fee))>10 or abs(sum(sum_commission))>10, max(0 + `10-全保费`),
       if(max(0 + `10-全保费`) > 0,
          max(0 + `10-全保费`) + if(min(0 + `10-全保费`) < 0, min(0 + `10-全保费`), 0),
          min(0 + `10-全保费`)))/1.06            as `11-净保费`,
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
       group_concat(distinct(`40-代理人名称`))                                     as `40-代理人名称`,
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
from result_#_final t,(SELECT @rownum:=0) temp
group by `6-保单单号`, if(`8-险种名称` in ('交强险', '商业险'), `8-险种名称`, 'ODS'), `4-发票付款方（与发票一致）`
'''

    String queryResult = "select * from result_#_3_final"

    void run(String type) {
        log.info("输出 合并匹配final3 result:{}", type)
//        result(type)
        result2(type)
        initData.fixPremium(type, getTableFinal())
        fix(type)
        log.info("输出 合并匹配final3 result完成:{}", type)
    }

}
