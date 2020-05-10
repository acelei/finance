package com.cheche365.service

import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
@Slf4j
class DataCombine {
    @Autowired
    Sql baseSql
    @Autowired
    InitData initData
    @Autowired
    TabSideData tabSideData

    private static final List columns = ["id", "s_id", "d_id", "c_id", "source_file", "1-序号", "2-保代机构", "3-出单保险代理机构（车车科技适用）", "4-发票付款方（与发票一致）", "5-投保人名称", "6-保单单号", "7-出单保险公司（明细至保险公司分支机构）", "8-险种名称", "9-保单出单日期", "10-全保费", "11-净保费", "12-手续费等级（对应点位台账）", "13-手续费率", "14-手续费总额（报行内+报行外）(含税)", "15-手续费总额（报行内+报行外）(不含税)", "16-收入入账月度", "17-凭证号", "18-手续费比例", "19-手续费金额（含税）", "20-手续费金额（不含税）", "21-回款月度", "22-凭证号", "23-收款金额", "24-开票单位", "25-开票日期", "26-手续费比例", "27-开票金额（不含税）", "28-开票金额（含税）", "29-20191231应收账款（含已开票和未开票）", "30-开票日期", "31-应收回款月度", "32-收款凭证号", "33-收款金额", "34-开票单位", "35-开票日期", "36-开票金额（不含税）", "37-开票金额（含税）", "38-尚未开票金额（不含税）", "39-尚未开票金额（含税）", "40-代理人名称", "41-佣金比例", "42-佣金金额（已入账）", "43-支付主体", "44-支付比例", "45-支付金额", "46-未计提佣金（19年底尚未入帐）", "保险公司", "省", "市", "保险公司id", "sum_fee", "sum_commission"]
    private static final StringJoiner valuesJoiner = new StringJoiner(",", "(", ")")
    static {
        columns.each { valuesJoiner.add("`${it}`") }
    }

    String settlementQuery = "select * from settlement_# "
    String commissionQuery = "select * from commission_# "
    String cleanSql = "truncate result_#"
    String insertSql = "insert into result_# "

    void result(String type) {
        List result = new ArrayList()
        log.info("查询结算数据:${type}")
        Map settlementGroup = queryDataGroup(getSettlementQuery().replace("#", type))
        log.info("查询付佣数据:${type}")
        Map commissionGroup = queryDataGroup(getCommissionQuery().replace("#", type))
        log.info("数据配对:${type}")
        combine(settlementGroup, commissionGroup, result)
        log.info("清除result:${type}")
        baseSql.execute(getCleanSql().replace("#", type))
        log.info("数据写入result:${type}")
        insertSql(getInsertSql().replace("#", type), result)
        log.info("数据写入result完成:${type}")
    }

    Map queryDataGroup(String sql) {
        baseSql.rows(sql).groupBy {
            if (it.'8-险种名称' == '交强险' || it.'8-险种名称' == '商业险') {
                return it.'6-保单单号' + it.'8-险种名称'
            }
            return it.'6-保单单号'
        }
    }

    static List combine(Map settlementMap, Map commissionMap, List result) {
        Set keySet = new HashSet()
        keySet.addAll(settlementMap.keySet())
        keySet.addAll(commissionMap.keySet())

        keySet.each { item ->
            List settlement = settlementMap.get(item) as List ?: []
            List commission = commissionMap.get(item) as List ?: []
            for (int i = 0; i < settlement.size(); i++) {
                def s = settlement.get(i)
                if (i < commission.size()) {
                    def c = commission.get(i)
                    s.'40-代理人名称' = c.'40-代理人名称'
                    s.'41-佣金比例' = c.'41-佣金比例'
                    s.'42-佣金金额（已入账）' = c.'42-佣金金额（已入账）'
                    s.'43-支付主体' = c.'43-支付主体'
                    s.'44-支付比例' = c.'44-支付比例'
                    s.'45-支付金额' = c.'45-支付金额'
                    s.'c_id' = c.'id'
                    s.'source_file' = s.'source_file' + ',' + c.'source_file'
                    s.sum_commission = c.sum_commission
                }
                s.'s_id' = s.'id'
                s.'id' = null

                result.add(s)
            }

            for (int i = settlement.size(); i < commission.size(); i++) {
                def c = commission.get(i)
                c.'c_id' = c.'id'
                c.'id' = null
                c.'3-出单保险代理机构（车车科技适用）' = c.'7-出单保险公司（明细至保险公司分支机构）'
                c.'4-发票付款方（与发票一致）' = c.'7-出单保险公司（明细至保险公司分支机构）'
                result.add(c)
            }
        }
        return result
    }

    void insertSql(String sql, List result) {
        List<StringJoiner> valueList = new ArrayList<>()

        sql = sql + valuesJoiner.toString() + " values "

        for (int i = 0; i < result.size(); i++) {
            def it = result.get(i)
            StringJoiner values = new StringJoiner(",", "(", ")")
            columns.each { item ->
                def value = it[item]
                if (value == null) {
                    values.add("null")
                }else {
                    values.add("'${(it[item] as String).replace("'","\\'")}'")
                }
            }
            valueList.add(values)

            if (i % 5000 == 0) {
                baseSql.executeInsert((sql + valueList.join(',')))
                valueList.clear()
            }
        }

        baseSql.executeInsert((sql + valueList.join(',')))
    }

    String clean2Sql = 'truncate result_#_2'
    String result2Sql = '''
insert into result_#_2 (
       s_id,
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
       `13-手续费率`,
       `14-手续费总额（报行内+报行外）(含税)`,
       `15-手续费总额（报行内+报行外）(不含税)`,
       `19-手续费金额（含税）`,
       `20-手续费金额（不含税）`,
       `23-收款金额`,
       `27-开票金额（不含税）`,
       `28-开票金额（含税）`,
       `29-20191231应收账款（含已开票和未开票）`,
       `33-收款金额`,
       `36-开票金额（不含税）`,
       `37-开票金额（含税）`,
       `38-尚未开票金额（不含税）`,
       `39-尚未开票金额（含税）`,
       `40-代理人名称`,
       `41-佣金比例`,
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
       gross_profit
)
select group_concat(s_id)                          as s_id,
       group_concat(c_id)                          as c_id,
       source_file,
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
       `12-手续费等级（对应点位台账）`,
       `13-手续费率`,
       sum(`14-手续费总额（报行内+报行外）(含税)`)     as `14-手续费总额（报行内+报行外）(含税)`,
       sum(`15-手续费总额（报行内+报行外）(不含税)`)    as `15-手续费总额（报行内+报行外）(不含税)`,
       sum(`19-手续费金额（含税）`)              as `19-手续费金额（含税）`,
       sum(`20-手续费金额（不含税）`)             as `20-手续费金额（不含税）`,
       sum(`23-收款金额`)                   as `23-收款金额`,
       sum(`27-开票金额（不含税）`)              as `27-开票金额（不含税）`,
       sum(`28-开票金额（含税）`)               as `28-开票金额（含税）`,
       sum(`29-20191231应收账款（含已开票和未开票）`) as `29-20191231应收账款（含已开票和未开票）`,
       sum(`33-收款金额`)                   as `33-收款金额`,
       sum(`36-开票金额（不含税）`)              as `36-开票金额（不含税）`,
       sum(`37-开票金额（含税）`)               as `37-开票金额（含税）`,
       sum(`38-尚未开票金额（不含税）`)            as `38-尚未开票金额（不含税）`,
       sum(`39-尚未开票金额（含税）`)             as `39-尚未开票金额（含税）`,
       max(`40-代理人名称`) as `40-代理人名称`,
       `41-佣金比例`,
       sum(`42-佣金金额（已入账）`)              as `42-佣金金额（已入账）`,
       `43-支付主体`,
       sum(`45-支付金额`)                   as `45-支付金额`,
       sum(`46-未计提佣金（19年底尚未入帐）`)        as `46-未计提佣金（19年底尚未入帐）`,
       `保险公司`,
       `保险公司id`,
       `省`,
       `市`,
       sum(sum_fee) as sum_fee,
       sum(sum_commission) as sum_commission,
       (sum(sum_fee)-sum(sum_commission))/sum(sum_fee) as gross_profit
from (
         select id   as s_id,
                null as c_id,
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
                `13-手续费率`,
                `14-手续费总额（报行内+报行外）(含税)`,
                `15-手续费总额（报行内+报行外）(不含税)`,
                `16-收入入账月度`,
                `17-凭证号`,
                `18-手续费比例`,
                `19-手续费金额（含税）`,
                `20-手续费金额（不含税）`,
                `21-回款月度`,
                `22-凭证号`,
                `23-收款金额`,
                `24-开票单位`,
                `25-开票日期`,
                `26-手续费比例`,
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
                null as `40-代理人名称`,
                null as `41-佣金比例`,
                0 as `42-佣金金额（已入账）`,
                null as `43-支付主体`,
                null as `44-支付比例`,
                0 as `45-支付金额`,
                0 as `46-未计提佣金（19年底尚未入帐）`,
                `保险公司`,
                `省`,
                `市`,
                `保险公司id`,
                sum_fee,
                0 as sum_commission
         from settlement_#
         union all
         select null as s_id,
                id   as c_id,
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
                null as `12-手续费等级（对应点位台账）`,
                null as `13-手续费率`,
                0 as `14-手续费总额（报行内+报行外）(含税)`,
                0 as `15-手续费总额（报行内+报行外）(不含税)`,
                null as `16-收入入账月度`,
                null as `17-凭证号`,
                null as `18-手续费比例`,
                0 as `19-手续费金额（含税）`,
                0 as `20-手续费金额（不含税）`,
                null as `21-回款月度`,
                null as `22-凭证号`,
                0 as `23-收款金额`,
                null as `24-开票单位`,
                null as `25-开票日期`,
                null as `26-手续费比例`,
                0 as `27-开票金额（不含税）`,
                0 as `28-开票金额（含税）`,
                0 as `29-20191231应收账款（含已开票和未开票）`,
                null as `30-开票日期`,
                null as `31-应收回款月度`,
                null as `32-收款凭证号`,
                0 as `33-收款金额`,
                null as `34-开票单位`,
                null as `35-开票日期`,
                0 as `36-开票金额（不含税）`,
                0 as `37-开票金额（含税）`,
                0 as `38-尚未开票金额（不含税）`,
                0 as `39-尚未开票金额（含税）`,
                `40-代理人名称`,
                `41-佣金比例`,
                `42-佣金金额（已入账）`,
                `43-支付主体`,
                `44-支付比例`,
                `45-支付金额`,
                `46-未计提佣金（19年底尚未入帐）`,
                `保险公司`,
                `省`,
                `市`,
                `保险公司id`,
                0 as sum_fee,
                sum_commission
         from commission_#) t
group by `6-保单单号`, if(`8-险种名称` in ('交强险', '商业险'), `8-险种名称`, 'ODS')
'''

    void result2(String type) {
        log.info("清除result2表:{}", type)
        baseSql.execute(getClean2Sql().replace('#', type))
        log.info("写入result2表:{}", type)
        baseSql.executeInsert(getResult2Sql().replace("#", type))
        log.info("写入result2表完成:{}", type)
    }
}
