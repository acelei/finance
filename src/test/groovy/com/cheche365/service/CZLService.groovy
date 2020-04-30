package com.cheche365.service

import app.SpringApplicationLauncher
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringApplicationLauncher.class)
@Slf4j
class CZLService {
    @Autowired
    private Sql baseSql

    List<Map<String, String>> types = [
            [source: "anhui_czl_keji", target: "anhui", type: "1"],
            [source: "dongguan_czl_keji", target: "dongguan", type: "7"],
            [source: "foshan_czl_keji", target: "foshan", type: "9"],
            [source: "fujian_czl_keji", target: "fujian", type: "10"],
            [source: "guangdong_czl_keji", target: "guangdong", type: "11"],
            [source: "guangxi_czl_keji", target: "guangxi", type: "12"],
            [source: "jiangsu_czl_keji", target: "jiangsu", type: "21"],
            [source: "shandong_czl_keji", target: "shandong", type: "91"],
            [source: "zhejiang_cal_keji", target: "zhejiang", type: "46"],
            [source: "liaoning_dsf_keji", target: "liaoning", type: "26"],
            [source: "sichuan_dsf_keji", target: "sichuan", type: "33"]
    ]

    String sTableName = 'settlement_kj_czl'
    String cTableName = 'commission_kj_czl'

    void back(String type) {
        baseSql.execute("create table settlement_${type}_back like settlement_${type}" as String)
        baseSql.executeInsert("insert into settlement_${type}_back select * from settlement_${type}" as String)
        baseSql.execute("create table commission_${type}_back like commission_${type}" as String)
        baseSql.executeInsert("insert into commission_${type}_back select * from commission_${type}" as String)
    }

    void delete(String type, String bdType) {
        baseSql.executeUpdate("delete a.* from settlement_${type} a,settlement_${bdType} b where b.source_file='settlement_${bdType}_2' and a.id=b.c_id" as String)
        baseSql.executeUpdate("delete a.* from commission_${type} a,commission_${bdType} b where b.source_file='commission_${bdType}_2' and a.id=b.c_id" as String)
    }

    void clean(String bdType) {
        baseSql.executeUpdate("delete from ${sTableName} where source_file='${bdType}'" as String)
        baseSql.executeUpdate("delete from ${cTableName} where source_file='${bdType}'" as String)
    }

    void getCZL(String bdType) {
        String sColumn = "null as id,`s_id`,`d_id`,`c_id`,`source_file`,`1-序号`,`2-保代机构`,`3-出单保险代理机构（车车科技适用）`,`4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`12-手续费等级（对应点位台账）`,`13-手续费率`,`14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,`16-收入入账月度`,`17-凭证号`,`18-手续费比例`,`19-手续费金额（含税）`,`20-手续费金额（不含税）`,`21-回款月度`,`22-凭证号`,`23-收款金额`,`24-开票单位`,`25-开票日期`,`26-手续费比例`,`27-开票金额（不含税）`,`28-开票金额（含税）`,`29-20191231应收账款（含已开票和未开票）`,`30-开票日期`,`31-应收回款月度`,`32-收款凭证号`,`33-收款金额`,`34-开票单位`,`35-开票日期`,`36-开票金额（不含税）`,`37-开票金额（含税）`,`38-尚未开票金额（不含税）`,`39-尚未开票金额（含税）`,`40-代理人名称`,`41-佣金比例`,`42-佣金金额（已入账）`,`43-支付主体`,`44-支付比例`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,`保险公司`,`省`,`市`,`保险公司id`,`handle_sign`,`sum_fee`,`sum_commission`,`gross_profit`,'settlement_${bdType}' as table_name,id as table_id"
        String cColumn = "null as id,`s_id`,`d_id`,`c_id`,`source_file`,`1-序号`,`2-保代机构`,`3-出单保险代理机构（车车科技适用）`,`4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`12-手续费等级（对应点位台账）`,`13-手续费率`,`14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,`16-收入入账月度`,`17-凭证号`,`18-手续费比例`,`19-手续费金额（含税）`,`20-手续费金额（不含税）`,`21-回款月度`,`22-凭证号`,`23-收款金额`,`24-开票单位`,`25-开票日期`,`26-手续费比例`,`27-开票金额（不含税）`,`28-开票金额（含税）`,`29-20191231应收账款（含已开票和未开票）`,`30-开票日期`,`31-应收回款月度`,`32-收款凭证号`,`33-收款金额`,`34-开票单位`,`35-开票日期`,`36-开票金额（不含税）`,`37-开票金额（含税）`,`38-尚未开票金额（不含税）`,`39-尚未开票金额（含税）`,`40-代理人名称`,`41-佣金比例`,`42-佣金金额（已入账）`,`43-支付主体`,`44-支付比例`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,`保险公司`,`省`,`市`,`保险公司id`,`handle_sign`,`sum_fee`,`sum_commission`,`gross_profit`,'commission_${bdType}' as table_name,id as table_id"
        baseSql.executeInsert("insert into ${sTableName} select ${sColumn} from settlement_${bdType} where source_file in ('settlement_${bdType}_2','settlement_sbt_czl_2','科技超自律', '赛博坦超自律')" as String)
        baseSql.executeInsert("insert into ${cTableName} select ${cColumn} from commission_${bdType} where source_file in ('commission_${bdType}_2','settlement_sbt_czl_2','科技超自律', '赛博坦超自律')" as String)
    }

    String settlementQuery = '''
select null as `id`,
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
       a.handle_sign,
       a.sum_fee,
       a.sum_commission,
       a.gross_profit,
       'settlement_#' as table_name,
       a.id as table_id
from settlement_# a
         left join business_replace_ref b on a.id = b.finance_id and b.table_name in ('settlement_#')
         left join result_gross_margin_ref c on a.id = c.s_id and c.type in (2,3)  and c.table_name in ('result_#_2', 'settlement_#', 'commission_#')
         left join commission_# d on d.id = c.c_id and c.type = 2
         left join settlement_# e on e.id = c.c_id and c.type = 3
         where a.source_file in ('settlement_#_2','settlement_sbt_czl_2','科技超自律', '赛博坦超自律')
'''
    String commissionQuery = '''
select null as `id`,
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
       a.handle_sign,
       a.sum_fee,
       a.sum_commission,
       a.gross_profit,
       'commission_#' as table_name,
       a.id as table_id
from commission_# a
         left join business_replace_ref b on a.id = b.finance_id and b.table_name in ('commission_#')
         left join result_gross_margin_ref c on a.id = c.c_id and c.type in (1,4) and c.table_name in ('result_#_2', 'settlement_#', 'commission_#')
         left join settlement_# d on d.id = c.s_id and c.type = 1
         left join commission_# e on e.id = c.s_id and c.type = 4
         where a.source_file in ('commission_#_2','settlement_sbt_czl_2','科技超自律', '赛博坦超自律')
'''

    void getFixCZL(String bdType) {
        baseSql.executeInsert(("insert into ${sTableName} " + settlementQuery.replace("#", bdType)) as String)
        baseSql.executeInsert(("insert into ${cTableName} " + commissionQuery.replace("#", bdType)) as String)
    }

    void roll(String type) {
        baseSql.execute("truncate settlement_${type}" as String)
        baseSql.executeInsert("insert into settlement_${type} select * from settlement_${type}_back" as String)
        baseSql.execute("truncate commission_${type}" as String)
        baseSql.executeInsert("insert into commission_${type} select * from commission_${type}_back" as String)
    }

    void fix() {
        baseSql.executeUpdate("update ${sTableName} set `23-收款金额`=`28-开票金额（含税）`" as String)
        baseSql.executeUpdate("update ${sTableName} set `27-开票金额（不含税）`=0,`28-开票金额（含税）`=0" as String)
        baseSql.executeUpdate("update ${sTableName} set `33-收款金额`=`37-开票金额（含税）`" as String)
        baseSql.executeUpdate("update ${sTableName} set `37-开票金额（含税）`=0,`38-尚未开票金额（不含税）`=0" as String)
        baseSql.executeUpdate("update ${sTableName} set `39-尚未开票金额（含税）`=0" as String)
        baseSql.executeUpdate("update ${sTableName} set `29-20191231应收账款（含已开票和未开票）`=`33-收款金额`+`37-开票金额（含税）`+`39-尚未开票金额（含税）`" as String)
        baseSql.executeUpdate("update ${sTableName} set `14-手续费总额（报行内+报行外）(含税)`=`23-收款金额`+`28-开票金额（含税）`+`29-20191231应收账款（含已开票和未开票）`" as String)
        baseSql.executeUpdate("update ${sTableName} set `15-手续费总额（报行内+报行外）(不含税)`=`14-手续费总额（报行内+报行外）(含税)`/1.06" as String)
        baseSql.executeUpdate("update ${sTableName} set `19-手续费金额（含税）`=`14-手续费总额（报行内+报行外）(含税)`" as String)
        baseSql.executeUpdate("update ${sTableName} set `20-手续费金额（不含税）`=`15-手续费总额（报行内+报行外）(不含税)`" as String)

        baseSql.executeUpdate("update ${cTableName} set `42-佣金金额（已入账）`=`45-支付金额`" as String)
        baseSql.executeUpdate("update ${cTableName} set `45-支付金额`=0" as String)

    }

    @Test
    void run() {
        getFixCZL('bj')
    }

    @Test
    void reRun() {
        def rows = baseSql.rows("select `type` from table_type where org!='科技' and flag>0")
        rows.each {
            def type = it.type
//            getCZL(type as String)
            getFixCZL(type as String)
        }
    }

    @Test
    void runFix() {
        fix()
    }
}
