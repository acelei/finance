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

    void getCZL(String bdType) {
        String column = "null as id,`s_id`,`d_id`,`c_id`,'${bdType}' as `source_file`,`1-序号`,`2-保代机构`,`3-出单保险代理机构（车车科技适用）`,`4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`12-手续费等级（对应点位台账）`,`13-手续费率`,`14-手续费总额（报行内+报行外）(含税)`,`15-手续费总额（报行内+报行外）(不含税)`,`16-收入入账月度`,`17-凭证号`,`18-手续费比例`,`19-手续费金额（含税）`,`20-手续费金额（不含税）`,`21-回款月度`,`22-凭证号`,`23-收款金额`,`24-开票单位`,`25-开票日期`,`26-手续费比例`,`27-开票金额（不含税）`,`28-开票金额（含税）`,`29-20191231应收账款（含已开票和未开票）`,`30-开票日期`,`31-应收回款月度`,`32-收款凭证号`,`33-收款金额`,`34-开票单位`,`35-开票日期`,`36-开票金额（不含税）`,`37-开票金额（含税）`,`38-尚未开票金额（不含税）`,`39-尚未开票金额（含税）`,`40-代理人名称`,`41-佣金比例`,`42-佣金金额（已入账）`,`43-支付主体`,`44-支付比例`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,`保险公司`,`省`,`市`,`保险公司id`,`handle_sign`,`sum_fee`,`sum_commission`,`gross_profit`"
        baseSql.executeInsert("insert into ${sTableName} select ${column} from settlement_${bdType} where source_file='settlement_${bdType}_2'" as String)
        baseSql.executeInsert("insert into ${cTableName} select ${column} from commission_${bdType} where source_file='commission_${bdType}_2'" as String)
    }

    void roll(String type) {
        baseSql.execute("truncate settlement_${type}" as String)
        baseSql.executeInsert("insert into settlement_${type} select * from settlement_${type}_back" as String)
        baseSql.execute("truncate commission_${type}" as String)
        baseSql.executeInsert("insert into commission_${type} select * from commission_${type}_back" as String)
    }

    @Test
    void run() {
        types.each {
//            back(it.source)
//            delete(it.source, it.target)
//            getCZL(it.target)
        }
    }

    @Test
    void reRun() {
        types.each {
            roll(it.source)
            delete(it.source, it.target)
            getCZL(it.target)
        }
    }
}
