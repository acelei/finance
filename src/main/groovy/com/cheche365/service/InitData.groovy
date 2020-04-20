package com.cheche365.service

import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.apache.commons.collections.CollectionUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
@Slf4j
class InitData {
    private List tables = ["commission", "settlement"]
    @Autowired
    private Sql baseSql
    @Autowired
    private FixInsuranceCompanyArea fixInsuranceCompanyArea

    private String updateHandleSignByInsPro = "update business_replace_ref t1\n" +
            "      inner join `tableNameVal` t2\n" +
            "      on t1.business_id = t2.id\n" +
            "      set t2.handle_sign = 0\n" +
            "      where t1.table_name in (\n" +
            "       'settlementTableNameVal',\n" +
            "       'commissionTableNameVal'\n" +
            "      )\n" +
            "      and t1.insurance_company_id = insuranceCompanyIdVal\n" +
            "      and t1.province_id = provinceIdVal"

    private String updateHandleSignByFinanceName = "update business_replace_ref t1 inner join das_data_pool_business t2 on t1.business_id = t2.id set t2.handle_sign = 0 where t1.table_name in ('settlementTableNameVal', 'commissionTableNameVal')"
    private String deleteBusinessRef = "delete from business_replace_ref where table_name in ('settlementTableNameVal', 'commissionTableNameVal')"

    private static final Long RENBAO = 2005L;

    void run(String type) {
        // 合计收入成本
        sumSettlementCommission(type)
        // 删除无效数据
        deleteNullData(type)
        // 处理保单号
        fixPolicyNo(type)
        // 处理险种
        fixInsuranceType(type)
        // 处理保险公司及地区
        fixInsuranceCompanyArea.run(type)
    }

    private static final policyNoSql = '''update # set `8-险种名称`=replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(trim(`8-险种名称`),'’',''),' ',''),'‘',''),'，',''),CHAR(39),''),',',''),',',''),CHAR(9), ''),CHAR(10), ''),CHAR(13),''),
`6-保单单号`=replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(trim(`6-保单单号`),'’',''),' ',''),'‘',''),'，',''),CHAR(39),''),',',''),',',''),CHAR(9), ''),CHAR(10), ''),CHAR(13),'')
'''

    void fixPolicyNo(String type) {
        log.info("整理保单号:{}", type)
        String
        tables.each {
            baseSql.executeUpdate(policyNoSql.replace("#", it + "_" + type))

        }
        log.info("整理保单号完成:{}", type)
    }

    private static final String jx = '''
update #
set `8-险种名称`='交强险'
where `8-险种名称` in (
'MP01000002机动车交通事故责任强制保险',
'机动车交强险',
'交强险(08版)',
'交强',
'摩托车交通事故责任强制保险',
'机动车交强险',
'机动车交通事故责任强制保险',
'机动车交通事故责任强制保险(0330)',
'机动车交通事故责任强制保险2009',
'机动车交通事故责任强制险',
'机动车机动车交强险',
 '汽车交通事故责任强制保险'
)
'''

    private static final String sj = '''
update #
set `8-险种名称`='商业险'
where `8-险种名称` in (
'MP01000001机动车辆商业保险',
'机动车商业险',
'商业',
'商业保险',
'商业险）',
'机动车商业保险（2014版）',
'机动车商业保险（2015版）',
'机动车商业保险（2016版）',
'机动车商业保险（2017版）',
'机动车商业保险（2018版）',
'机动车商业保险（2019版）',
'机动车商业保险（2020版）',
'机动车商业保险（2021版）',
'机动车商业保险（2022版）',
'机动车商业保险（2023版）',
'机动车商业保险（2024版）',
'机动车商业保险（2025版）',
'机动车商业行业示范汽车保险',
'机动车商业险',
'机动车综合商业保险',
'机动车综合商业保险示范条款（2014版）',
'机动车商业综合条款',
'机动车商业险（2015）',
'机动车综合商业保险示范条款（2014版）(0336)',
'机动车综合险2014版',
'机动车机动车商业险',
'机动车辆商业保险',
'特种车综合商业保险',
'机动车商业行业示范汽车保险条款'
)
'''

    void fixInsuranceType(String type) {
        log.info("整理险种:{}", type)
        tables.each {
            baseSql.executeUpdate(jx.replace("#", it + "_" + type))
            baseSql.executeUpdate(sj.replace("#", it + "_" + type))
        }
        log.info("整理险种完成:{}", type)
    }

    private static final String settlementSum = "update settlement_# set `14-手续费总额（报行内+报行外）(含税)`=ifnull(`14-手续费总额（报行内+报行外）(含税)`,0),`15-手续费总额（报行内+报行外）(不含税)`=ifnull(`15-手续费总额（报行内+报行外）(不含税)`,0),sum_fee=ifnull(`14-手续费总额（报行内+报行外）(含税)`,0),sum_commission=0"
    private static final String commissionSum = "update commission_# set sum_fee=0,`42-佣金金额（已入账）`=ifnull(`42-佣金金额（已入账）`,0),`45-支付金额`=ifnull(`45-支付金额`,0),`46-未计提佣金（19年底尚未入帐）`=ifnull(`46-未计提佣金（19年底尚未入帐）`,0),sum_commission=(ifnull(`42-佣金金额（已入账）`,0)+ifnull(`45-支付金额`,0)+ifnull(`46-未计提佣金（19年底尚未入帐）`,0))"

    void sumSettlementCommission(String type) {
        log.info("合计收入成本:{}", type)
        baseSql.executeUpdate(settlementSum.replace("#", type))
        baseSql.executeUpdate(commissionSum.replace("#", type))
        log.info("合计收入成本完成:{}", type)
    }


    private static final String settlementSql = "delete from settlement_# where `14-手续费总额（报行内+报行外）(含税)`=0 and `15-手续费总额（报行内+报行外）(不含税)`=0"
    private static final String commissionSql = "delete from commission_# where `42-佣金金额（已入账）`=0 and `45-支付金额`=0 and `46-未计提佣金（19年底尚未入帐）`=0"

    void deleteNullData(String type) {
        log.info("删除无效数据:{}", type)
        baseSql.executeUpdate(settlementSql.replace("#", type))
        baseSql.executeUpdate(commissionSql.replace("#", type))
        log.info("删除无效数据完成:{}", type)
    }

    void clean(String type) {
        log.info("清除状态标记:${type}")
        baseSql.executeUpdate("update settlement_# set d_id=null,s_id=null,c_id=null,handle_sign=0".replace("#", type))
        baseSql.executeUpdate("update commission_# set d_id=null,s_id=null,c_id=null,handle_sign=0".replace("#", type))
        baseSql.executeUpdate("delete from result_gross_margin_ref where table_name in ('result_#_2','commission_#','settlement_#')".replace("#", type))
        cleanReplaceData(type)
        log.info("清除状态标记:${type}")
    }

    void cleanReplaceData(String type) {
        String commissionTableName = "commission_" + type
        String settleMentTableName = "settlement_" + type
        List<GroovyRowResult> insProList = baseSql.rows("select insurance_company_id as insuranceCompanyId, province_id as provinceId from business_replace_ref " +
                "where table_name in ('" + commissionTableName + "','" + settleMentTableName + "') group by insurance_company_id, province_id")
        if (CollectionUtils.isNotEmpty(insProList)) {
            for (GroovyRowResult map : insProList) {
                String insuranceCompanyId = map.get("insuranceCompanyId").toString()
                String provinceId = map.get("provinceId").toString();
                baseSql.executeUpdate(updateHandleSignByInsPro.replace("insuranceCompanyIdVal", insuranceCompanyId).replace("provinceIdVal", provinceId)
                    .replace("commissionTableNameVal", commissionTableName).replace("settlementTableNameVal", settleMentTableName)
                    .replace("tableNameVal", generInsuranceCompany(insuranceCompanyId, provinceId)))
            }

            baseSql.executeUpdate(updateHandleSignByFinanceName.replace("settlementTableNameVal", settleMentTableName).replace("commissionTableNameVal", commissionTableName))
            baseSql.execute(deleteBusinessRef.replace("settlementTableNameVal", settleMentTableName).replace("commissionTableNameVal", commissionTableName))
            log.info("resetBusinessByName success! type:{}", type)
        }
    }

    private String generInsuranceCompany(String insuranceCompanyId, String provinceId) {
        if (insuranceCompanyId.equals(RENBAO) && provinceId != null) {
            return "das_data_pool_business_" + insuranceCompanyId + "_" + provinceId;
        } else {
            return "das_data_pool_business_" + insuranceCompanyId;
        }
    }

    private static final String backSql = '''insert into result_#_back (d_id,s_id,c_id,`2-保代机构`,`3-出单保险代理机构（车车科技适用）`,`4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`12-手续费等级（对应点位台账）`,`13-手续费率`,
            `14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,`19-手续费金额（含税）`,`20-手续费金额（不含税）`,`23-收款金额`,`27-开票金额（不含税）`,`28-开票金额（含税）`,`29-20191231应收账款（含已开票和未开票）`,`33-收款金额`,`36-开票金额（不含税）`,`37-开票金额（含税）`,`38-尚未开票金额（不含税）`,`39-尚未开票金额（含税）`,
            `42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,`保险公司`,`保险公司id`,`省`,`市`,handle_sign,sum_fee,sum_commission,gross_profit) select d_id,s_id,c_id,`2-保代机构`,`3-出单保险代理机构（车车科技适用）`,`4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`12-手续费等级（对应点位台账）`,`13-手续费率`,
            `14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,`19-手续费金额（含税）`,`20-手续费金额（不含税）`,`23-收款金额`,`27-开票金额（不含税）`,`28-开票金额（含税）`,`29-20191231应收账款（含已开票和未开票）`,`33-收款金额`,`36-开票金额（不含税）`,`37-开票金额（含税）`,`38-尚未开票金额（不含税）`,`39-尚未开票金额（含税）`,
            `42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,`保险公司`,`保险公司id`,`省`,`市`,handle_sign,sum_fee,sum_commission,gross_profit from result_#_2'''

    void back(String type) {
        log.info("清除back表:{}", type)
        baseSql.execute('truncate result_#_back'.replace('#', type))
        log.info("写入back表:{}", type)
        baseSql.executeInsert(backSql.replace("#", type))
        log.info("写入back表完成:{}", type)
    }

    private static final String rollSql = '''insert into result_#_2 (d_id,s_id,c_id,`2-保代机构`,`3-出单保险代理机构（车车科技适用）`,`4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`12-手续费等级（对应点位台账）`,`13-手续费率`,
            `14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,`19-手续费金额（含税）`,`20-手续费金额（不含税）`,`23-收款金额`,`27-开票金额（不含税）`,`28-开票金额（含税）`,`29-20191231应收账款（含已开票和未开票）`,`33-收款金额`,`36-开票金额（不含税）`,`37-开票金额（含税）`,`38-尚未开票金额（不含税）`,`39-尚未开票金额（含税）`,
            `42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,`保险公司`,`保险公司id`,`省`,`市`,handle_sign,sum_fee,sum_commission,gross_profit) select d_id,s_id,c_id,`2-保代机构`,`3-出单保险代理机构（车车科技适用）`,`4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`12-手续费等级（对应点位台账）`,`13-手续费率`,
            `14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,`19-手续费金额（含税）`,`20-手续费金额（不含税）`,`23-收款金额`,`27-开票金额（不含税）`,`28-开票金额（含税）`,`29-20191231应收账款（含已开票和未开票）`,`33-收款金额`,`36-开票金额（不含税）`,`37-开票金额（含税）`,`38-尚未开票金额（不含税）`,`39-尚未开票金额（含税）`,
            `42-佣金金额（已入账）`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,`保险公司`,`保险公司id`,`省`,`市`,handle_sign,sum_fee,sum_commission,gross_profit from result_#_back'''

    void roll(String type) {
        log.info("恢复back表:{}", type)
        baseSql.execute('truncate result_#_2'.replace('#', type))
        baseSql.executeInsert(rollSql.replace("#", type))
        log.info("恢复back表完成:{}", type)
        fixPremium(type, "")
        clean(type)
    }

    private static final List<String> fixPremiumSql = ['''
update result_#_2&
set `10-全保费`=if(abs(`10-全保费` - sum_fee) > abs(`11-净保费` - sum_fee), `11-净保费` * 1.06, `10-全保费`),
    `11-净保费`=if(abs(`10-全保费` - sum_fee) > abs(`11-净保费` - sum_fee), `11-净保费`, `10-全保费` / 1.06)
where abs(`14-手续费总额（报行内+报行外）(含税)`) > 0
  and abs(`10-全保费`)>0
  and abs(ROUND(`10-全保费`) - ROUND(`11-净保费` * 1.06)) > 1
order by abs(ROUND(`10-全保费`) - ROUND(`11-净保费` * 1.06))
''',
                                                       '''
update result_#_2&
set `10-全保费`=if(abs(`10-全保费` - sum_commission) >
                abs(`11-净保费` - sum_commission), `11-净保费` * 1.06, `10-全保费`),
    `11-净保费`=if(abs(`10-全保费` - sum_commission) >
                abs(`11-净保费` - sum_commission), `11-净保费`, `10-全保费` / 1.06)
where abs(`14-手续费总额（报行内+报行外）(含税)`) = 0
  and abs(`10-全保费`)>0
  and abs(ROUND(`10-全保费`) - ROUND(`11-净保费` * 1.06)) > 1
order by abs(ROUND(`10-全保费`) - ROUND(`11-净保费` * 1.06))
''',
                                                       '''update result_#_2& set `10-全保费`=0-`10-全保费`,`11-净保费`=0-`11-净保费` where abs(sum_fee)>0 and `10-全保费`>0 and sum_fee<0'''
                                                       , '''update result_#_2& set `10-全保费`=0-`10-全保费`,`11-净保费`=0-`11-净保费` where abs(sum_fee)>0 and `10-全保费`<0 and sum_fee>0'''
                                                       , '''update result_#_2& set `10-全保费`=0-`10-全保费`,`11-净保费`=0-`11-净保费` where abs(sum_fee)=0 and `10-全保费`>0 and sum_commission<0'''
                                                       , '''update result_#_2& set `10-全保费`=0-`10-全保费`,`11-净保费`=0-`11-净保费` where abs(sum_fee)=0 and `10-全保费`<0 and sum_commission>0''']

    void fixPremium(String type, String suffix) {
        log.info("在result2中修正保费:{}", type)
        suffix = suffix ?: ""
        fixPremiumSql.each {
            baseSql.executeUpdate(it.replace("#", type).replace("&", suffix))
        }
        log.info("在result2中修正保费完成:{}", type)
    }


    String result3 = '''
insert into result_#_3 (
       s_id,
       c_id,
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
       `2-保代机构`,
       `3-出单保险代理机构（车车科技适用）`,
       `4-发票付款方（与发票一致）`,
       `5-投保人名称`,
       `6-保单单号`,
       `7-出单保险公司（明细至保险公司分支机构）`,
       `8-险种名称`,
       `9-保单出单日期`,
       if(max(0 + ifnull(`10-全保费`, 0)) > 0,
          max(0 + ifnull(`10-全保费`, 0)) - if(min(0 + ifnull(`10-全保费`, 0)) < 0, min(0 + ifnull(`10-全保费`, 0)), 0),
          min(0 + ifnull(`10-全保费`, 0)))            as `10-全保费`,
       if(max(0 + ifnull(`11-净保费`, 0)) > 0,
          max(0 + ifnull(`11-净保费`, 0)) - if(min(0 + ifnull(`11-净保费`, 0)) < 0, min(0 + ifnull(`11-净保费`, 0)), 0),
          min(0 + ifnull(`11-净保费`, 0)))            as `11-净保费`,
       `12-手续费等级（对应点位台账）`,
       `13-手续费率`,
       sum(ifnull(`14-手续费总额（报行内+报行外）(含税)`, 0))     as `14-手续费总额（报行内+报行外）(含税)`,
       sum(ifnull(`15-手续费总额（报行内+报行外）(不含税)`, 0))    as `15-手续费总额（报行内+报行外）(不含税)`,
       sum(ifnull(`19-手续费金额（含税）`, 0))              as `19-手续费金额（含税）`,
       sum(ifnull(`20-手续费金额（不含税）`, 0))             as `20-手续费金额（不含税）`,
       sum(ifnull(`23-收款金额`, 0))                   as `23-收款金额`,
       sum(ifnull(`27-开票金额（不含税）`, 0))              as `27-开票金额（不含税）`,
       sum(ifnull(`28-开票金额（含税）`, 0))               as `28-开票金额（含税）`,
       sum(ifnull(`29-20191231应收账款（含已开票和未开票）`, 0)) as `29-20191231应收账款（含已开票和未开票）`,
       sum(ifnull(`33-收款金额`, 0))                   as `33-收款金额`,
       sum(ifnull(`36-开票金额（不含税）`, 0))              as `36-开票金额（不含税）`,
       sum(ifnull(`37-开票金额（含税）`, 0))               as `37-开票金额（含税）`,
       sum(ifnull(`38-尚未开票金额（不含税）`, 0))            as `38-尚未开票金额（不含税）`,
       sum(ifnull(`39-尚未开票金额（含税）`, 0))             as `39-尚未开票金额（含税）`,
       `40-代理人名称`,
       `41-佣金比例`,
       sum(ifnull(`42-佣金金额（已入账）`, 0))              as `42-佣金金额（已入账）`,
       `43-支付主体`,
       sum(ifnull(`45-支付金额`, 0))                   as `45-支付金额`,
       sum(ifnull(`46-未计提佣金（19年底尚未入帐）`, 0))        as `46-未计提佣金（19年底尚未入帐）`,
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
                null as `42-佣金金额（已入账）`,
                null as `43-支付主体`,
                null as `44-支付比例`,
                null as `45-支付金额`,
                null as `46-未计提佣金（19年底尚未入帐）`,
                `保险公司`,
                `省`,
                `市`,
                `保险公司id`,
                sum_fee,
                0 as sum_commission
         from settlement_#
         union all
         select null as s_ic,
                id   as c_id,
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
                null as `14-手续费总额（报行内+报行外）(含税)`,
                null as `15-手续费总额（报行内+报行外）(不含税)`,
                null as `16-收入入账月度`,
                null as `17-凭证号`,
                null as `18-手续费比例`,
                null as `19-手续费金额（含税）`,
                null as `20-手续费金额（不含税）`,
                null as `21-回款月度`,
                null as `22-凭证号`,
                null as `23-收款金额`,
                null as `24-开票单位`,
                null as `25-开票日期`,
                null as `26-手续费比例`,
                null as `27-开票金额（不含税）`,
                null as `28-开票金额（含税）`,
                null as `29-20191231应收账款（含已开票和未开票）`,
                null as `30-开票日期`,
                null as `31-应收回款月度`,
                null as `32-收款凭证号`,
                null as `33-收款金额`,
                null as `34-开票单位`,
                null as `35-开票日期`,
                null as `36-开票金额（不含税）`,
                null as `37-开票金额（含税）`,
                null as `38-尚未开票金额（不含税）`,
                null as `39-尚未开票金额（含税）`,
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
group by `6-保单单号`, if(`8-险种名称` in ('交强险', '商业险'), `8-险种名称`, 'ODS'), `4-发票付款方（与发票一致）`, `7-出单保险公司（明细至保险公司分支机构）`
'''

    void result3(String type) {
        log.info("清除result3表:{}", type)
        baseSql.execute("truncate result_#_3".replace("#", type))
        log.info("写入result3表:{}", type)
        baseSql.executeInsert(result3.replace("#", type))
        log.info("写入result3表完成:{}", type)
    }


    private static final String errSql1 = '''
update result_#_2
set handle_sign=3
where handle_sign in (0, 1, 4, 6)
  and `8-险种名称` in ('交强险', '商业险')
  and abs(sum_fee) > 0
  and (sum_fee / `11-净保费` < if(`8-险种名称` = '交强险', 0, 0.12) or
       sum_fee / `11-净保费` > 0.7)
  and date_format(`9-保单出单日期`,'%Y')='2019'
'''
    private static final String errSql2 = '''
update result_#_2
set handle_sign=3
where handle_sign in (0, 1, 4, 6)
  and `8-险种名称` in ('交强险', '商业险')
  and abs(sum_commission) > 0
  and (sum_commission / `11-净保费` < if(`8-险种名称` = '交强险', 0, 0.12) or
       sum_commission / `11-净保费` > 0.7)
  and date_format(`9-保单出单日期`,'%Y')='2019'
'''

    void flagErrData(String type) {
        log.info("设置保费比例问题标签:{}", type)
        baseSql.executeUpdate(errSql1.replace("#", type))
        baseSql.executeUpdate(errSql2.replace("#", type))
        log.info("设置保费比例问题标签完成:{}", type)
    }


    String[] strList = [
            "set a.s_id=b.s_id,a.type=4 where a.s_id=b.c_id and a.type=4 and b.type=4 and a.s_id<>b.s_id",
            "set a.s_id=b.c_id,a.type=1 where a.s_id=b.s_id and a.type=1 and b.type=3 and a.s_id<>b.c_id",
            "set a.s_id=b.s_id,a.type=1 where a.s_id=b.c_id and a.type=4 and b.type=1 and a.s_id<>b.s_id",
            "set a.s_id=b.c_id,a.type=4 where a.s_id=b.s_id and a.type=1 and b.type=2 and a.s_id<>b.c_id",
            "set a.c_id=b.c_id,a.type=3 where a.c_id=b.s_id and a.type=3 and b.type=3 and a.c_id<>b.c_id",
            "set a.c_id=b.s_id,a.type=3 where a.c_id=b.c_id and a.type=2 and b.type=1 and a.c_id<>b.s_id",
            "set a.c_id=b.c_id,a.type=2 where a.c_id=b.s_id and a.type=3 and b.type=2 and a.c_id<>b.c_id",
            "set a.c_id=b.s_id,a.type=2 where a.c_id=b.c_id and a.type=2 and b.type=4 and a.c_id<>b.s_id"
    ]
    String updateSql = "update result_gross_margin_ref a,result_gross_margin_ref b &str and a.table_name in ('result_#_2','settlement_#','commission_#') and b.table_name in ('result_#_2','settlement_#','commission_#')";

    void fixRef(String type) {
        log.info("修正配对关联:{}", type)
        strList.each {
            int n = 1
            while (n > 0) {
                n = baseSql.executeUpdate(updateSql.replace("#", type).replace("&str", it))
            }
        }
        log.info("修正配对关联完成:{}", type)
    }
}
