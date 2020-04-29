package com.cheche365.service

import app.SpringApplicationLauncher
import com.cheche365.util.ExcelUtil2
import com.cheche365.util.ThreadPool
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.apache.commons.io.FileUtils
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringApplicationLauncher.class)
@Slf4j
class MatchSPData {
    @Autowired
    private Sql baseSql
    @Autowired
    private ThreadPool runThreadPool

    // 备份广管机构数据
    @Test
    void step0() {
        List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where org='保代-广管'")
        for (GroovyRowResult row : rows) {
            log.info(row.type)
            baseSql.executeInsert("truncate settlement_#_back".replace("#", row.type));
            baseSql.executeInsert("truncate commission_#_back".replace("#", row.type));
            baseSql.executeInsert("insert into settlement_#_back select * from settlement_#".replace("#", row.type))
            baseSql.executeInsert("insert into commission_#_back select * from commission_#".replace("#", row.type))
        }
    }

    // 清除保代广管 24-28 34-37 45
    @Test
    void step0_1() {
        List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where org='保代-广管'")
        for (GroovyRowResult row : rows) {
            baseSql.executeInsert("update settlement_# set `24-开票单位`=null,`25-开票日期`=null,`26-手续费比例`=null,`27-开票金额（不含税）`=0,`28-开票金额（含税）`=0,`34-开票单位`=null,`35-开票日期`=null,`36-开票金额（不含税）`=0,`37-开票金额（含税）`=0".replace("#", row.type))
            baseSql.executeInsert("update commission_# set `45-支付金额`=0".replace("#", row.type))
        }
    }

    // 匹配总部数据
    @Test
    void step1() {
        baseSql.executeUpdate("update commission_zongbu_2 a,commission_zongbu b set a.flag=1,a.type_id=47 where a.`6-保单单号`=b.`6-保单单号` and a.`8-险种名称`=b.`8-险种名称` and a.flag=0");
        baseSql.executeUpdate("update commission_zongbu_2 a,settlement_zongbu b set a.flag=1,a.type_id=47 where a.`6-保单单号`=b.`6-保单单号` and a.`8-险种名称`=b.`8-险种名称` and a.flag=0");
    }

    @Test
    void step1_1() {
        baseSql.executeUpdate("update commission_zongbu_2 a,commission_zongbu b set a.flag=1,a.type_id=47,a.`8-险种名称`=b.`8-险种名称` where a.`6-保单单号`=b.`6-保单单号` and a.flag=0");
        baseSql.executeUpdate("update commission_zongbu_2 a,settlement_zongbu b set a.flag=1,a.type_id=47,a.`8-险种名称`=b.`8-险种名称` where a.`6-保单单号`=b.`6-保单单号` and a.flag=0");
    }

    //导出未匹配总部数据
    @Test
    void step1_2() {
        def rows = baseSql.rows("select * from commission_zongbu_2 where flag=0")
        if (rows.size() > 0) {
            log.info("总部-{}", rows.size())
            FileUtils.moveFile(ExcelUtil2.writeToExcel(head, rows), new File("总部.xlsx"))
        }
    }

    //匹配超自律数据
    @Test
    void step2() {
        List<Map<String, String>> types = [
                [source: "anhui_2", target: "anhui", type: "1"],
                [source: "dongguan_2", target: "dongguan", type: "7"],
                [source: "foshan_2", target: "foshan", type: "9"],
                [source: "fujian_2", target: "fujian", type: "10"],
                [source: "guangdong_2", target: "guangdong", type: "11"],
                [source: "guangxi_2", target: "guangxi", type: "12"],
                [source: "jiangsu_2", target: "jiangsu", type: "21"],
                [source: "shandong_2", target: "shandong", type: "91"],
                [source: "zhejiang_2", target: "zhejiang", type: "46"],
                [source: "liaoning_2", target: "liaoning", type: "26"],
                [source: "sichuan_2", target: "sichuan", type: "33"]
        ]
        runThreadPool.executeWithLatch(types, { type ->
            def sql = ["update settlement_${type.source} a,settlement_${type.target} b set a.flag=1,a.type_id=${type.type} where a.`6-保单单号`=b.`6-保单单号` and a.`8-险种名称`=b.`8-险种名称` and a.flag=0" as String,
                       "update settlement_${type.source} a,commission_${type.target} b set a.flag=1,a.type_id=${type.type} where a.`6-保单单号`=b.`6-保单单号` and a.`8-险种名称`=b.`8-险种名称` and a.flag=0" as String,
                       "update commission_${type.source} a,commission_${type.target} b set a.flag=1,a.type_id=${type.type} where a.`6-保单单号`=b.`6-保单单号` and a.`8-险种名称`=b.`8-险种名称` and a.flag=0" as String,
                       "update commission_${type.source} a,settlement_${type.target} b set a.flag=1,a.type_id=${type.type} where a.`6-保单单号`=b.`6-保单单号` and a.`8-险种名称`=b.`8-险种名称` and a.flag=0" as String]

            sql.each {
                baseSql.executeUpdate(it)
            }
        }).await()
    }

    void step2_1() {
        List<Map<String, String>> types = [
                [source: "anhui_2", target: "anhui", type: "1"],
                [source: "dongguan_2", target: "dongguan", type: "7"],
                [source: "foshan_2", target: "foshan", type: "9"],
                [source: "fujian_2", target: "fujian", type: "10"],
                [source: "guangdong_2", target: "guangdong", type: "11"],
                [source: "guangxi_2", target: "guangxi", type: "12"],
                [source: "jiangsu_2", target: "jiangsu", type: "21"],
                [source: "shandong_2", target: "shandong", type: "91"],
                [source: "zhejiang_2", target: "zhejiang", type: "46"],
                [source: "liaoning_2", target: "liaoning", type: "26"],
                [source: "sichuan_2", target: "sichuan", type: "33"]
        ]
        runThreadPool.executeWithLatch(types, { type ->
            def sql = ["update settlement_${type.source} a,settlement_${type.target} b set a.flag=1,a.type_id=${type.type},a.`8-险种名称`=b.`8-险种名称` where a.`6-保单单号`=b.`6-保单单号` and a.flag=0" as String,
                       "update settlement_${type.source} a,commission_${type.target} b set a.flag=1,a.type_id=${type.type},a.`8-险种名称`=b.`8-险种名称` where a.`6-保单单号`=b.`6-保单单号` and a.flag=0" as String,
                       "update commission_${type.source} a,commission_${type.target} b set a.flag=1,a.type_id=${type.type},a.`8-险种名称`=b.`8-险种名称` where a.`6-保单单号`=b.`6-保单单号` and a.flag=0" as String,
                       "update commission_${type.source} a,settlement_${type.target} b set a.flag=1,a.type_id=${type.type},a.`8-险种名称`=b.`8-险种名称` where a.`6-保单单号`=b.`6-保单单号` and a.flag=0" as String]

            sql.each {
                baseSql.executeUpdate(it)
            }
        }).await()
    }

    // 查看是否在科技边存在,错误检查
    @Test
    void step2_1_2() {
        List<Map<String, String>> types = [
                [source: "zhejiang_2", target: "zhejiang_dsf_keji", type: "75"],
                [source: "liaoning_2", target: "liaoning_dsf_keji", type: "68"],
                [source: "sichuan_2", target: "sichuan_dsf_keji", type: "71"],
                [source: "jiangsu_2", target: "jiangsu_dsf_keji", type: "67"],
                [source: "fujian_2", target: "fujian_dsf_keji", type: "61"],
        ]
        runThreadPool.executeWithLatch(types, { type ->
            def sql = ["update settlement_${type.source} a,settlement_${type.target} b set a.flag=2,a.type_id=${type.type} where a.`6-保单单号`=b.`6-保单单号` and a.`8-险种名称`=b.`8-险种名称` and a.flag=0" as String,
                       "update settlement_${type.source} a,commission_${type.target} b set a.flag=2,a.type_id=${type.type} where a.`6-保单单号`=b.`6-保单单号` and a.`8-险种名称`=b.`8-险种名称` and a.flag=0" as String,
                       "update commission_${type.source} a,commission_${type.target} b set a.flag=2,a.type_id=${type.type} where a.`6-保单单号`=b.`6-保单单号` and a.`8-险种名称`=b.`8-险种名称` and a.flag=0" as String,
                       "update commission_${type.source} a,settlement_${type.target} b set a.flag=2,a.type_id=${type.type} where a.`6-保单单号`=b.`6-保单单号` and a.`8-险种名称`=b.`8-险种名称` and a.flag=0" as String]

            sql.each {
                baseSql.executeUpdate(it)
            }
        }).await()
    }

    // 导出未匹配超自律数据
    List<String> head = ["id", "s_id", "d_id", "c_id", "source_file", "1-序号", "2-保代机构", "3-出单保险代理机构（车车科技适用）", "4-发票付款方（与发票一致）", "5-投保人名称", "6-保单单号", "7-出单保险公司（明细至保险公司分支机构）", "8-险种名称", "9-保单出单日期", "10-全保费", "11-净保费", "12-手续费等级（对应点位台账）", "13-手续费率", "14-手续费总额（报行内+报行外）(含税)", "15-手续费总额（报行内+报行外）(不含税)", "16-收入入账月度", "17-凭证号", "18-手续费比例", "19-手续费金额（含税）", "20-手续费金额（不含税）", "21-回款月度", "22-凭证号", "23-收款金额", "24-开票单位", "25-开票日期", "26-手续费比例", "27-开票金额（不含税）", "28-开票金额（含税）", "29-20191231应收账款（含已开票和未开票）", "30-开票日期", "31-应收回款月度", "32-收款凭证号", "33-收款金额", "34-开票单位", "35-开票日期", "36-开票金额（不含税）", "37-开票金额（含税）", "38-尚未开票金额（不含税）", "39-尚未开票金额（含税）", "40-代理人名称", "41-佣金比例", "42-佣金金额（已入账）", "43-支付主体", "44-支付比例", "45-支付金额", "46-未计提佣金（19年底尚未入帐）", "保险公司", "省", "市", "保险公司id", "handle_sign", "sum_fee", "sum_commission", "gross_profit"]

    @Test
    void step2_2() {
        List<String> types = ["anhui_2", "dongguan_2", "foshan_2", "fujian_2", "guangdong_2", "guangxi_2", "jiangsu_2", "shandong_2", "zhejiang_2", "liaoning_2", "sichuan_2"]
        runThreadPool.executeWithLatch(types, { type ->
            def r1 = baseSql.rows("select * from settlement_${type} where flag=0" as String)
            def r2 = baseSql.rows("select * from commission_${type} where flag=0" as String)
            log.info("{}:settlement-{},commission-{}", type, r1.size(), r2.size())
            if (r1.size() > 0) {
                FileUtils.moveFile(ExcelUtil2.writeToExcel(head, r1), new File("${type}结算.xlsx"))
            }
            if (r2.size() > 0) {
                FileUtils.moveFile(ExcelUtil2.writeToExcel(head, r2), new File("${type}付佣.xlsx"))
            }
        }).await()
    }

    // 统计
    @Test
    void step2_3() {
        List<String> types = ["anhui_2", "dongguan_2", "foshan_2", "fujian_2", "guangdong_2", "guangxi_2", "jiangsu_2", "shandong_2", "zhejiang_2", "liaoning_2", "sichuan_2"]
        List<Map> resultList = new ArrayList<>()
        runThreadPool.executeWithLatch(types, { type ->
            def r1 = baseSql.rows("select '${type}-结算' as 业务,if(flag=1,'保代匹配',if(flag=2,'科技匹配','未匹配')) as 类型,count(9) as 数量,sum(sum_fee) as 金额 from settlement_${type} group by flag" as String)
            def r2 = baseSql.rows("select '${type}-付佣' as 业务,if(flag=1,'保代匹配',if(flag=2,'科技匹配','未匹配')) as 类型,count(9) as 数量,sum(sum_commission) as 金额 from commission_${type} group by flag" as String)
            synchronized (resultList) {
                resultList.addAll(r1)
                resultList.addAll(r2)
            }
        }).await()

        ExcelUtil2.writeToExcel(['业务', '类型', '数量', '金额'], resultList).renameTo(new File("超自律匹配统计.xlsx"))
    }

    // 匹配科技付佣数据
    @Test
    void step3() {
        def rows = baseSql.rows("select `id`,`type` from table_type where flag>=0")
        rows.each { type ->
            baseSql.executeUpdate("update commission_all_2 a,commission_${type.type} b set a.flag=1,a.type_id=${type.id} where a.`6-保单单号`=b.`6-保单单号` and a.`8-险种名称`=b.`8-险种名称` and a.flag=0" as String)
            baseSql.executeUpdate("update commission_all_2 a,settlement_${type.type} b set a.flag=1,a.type_id=${type.id} where a.`6-保单单号`=b.`6-保单单号` and a.`8-险种名称`=b.`8-险种名称` and a.flag=0" as String)
        }
    }

    @Test
    void step3_1() {
        def rows = baseSql.rows("select `id`,`type` from table_type where flag>=0")
        rows.each { type ->
            baseSql.executeUpdate("update commission_all_2 a,commission_${type.type} b set a.flag=1,a.type_id=${type.id},a.`8-险种名称`=b.`8-险种名称` where a.`6-保单单号`=b.`6-保单单号` and a.flag=0" as String)
            baseSql.executeUpdate("update commission_all_2 a,settlement_${type.type} b set a.flag=1,a.type_id=${type.id},a.`8-险种名称`=b.`8-险种名称` where a.`6-保单单号`=b.`6-保单单号` and a.flag=0" as String)
        }
    }

    // 导出未匹配科技付佣数据
    @Test
    void step3_2() {
        def rows = baseSql.rows("select * from commission_all_2 where flag=0")
        if (rows.size() > 0) {
            log.info("科技付佣-{}", rows.size())
            FileUtils.moveFile(ExcelUtil2.writeToExcel(head, rows), new File("科技付佣未匹配.xlsx"))
        }
    }

    List<String> head2 = ["业务名称", "id", "s_id", "d_id", "c_id", "source_file", "1-序号", "2-保代机构", "3-出单保险代理机构（车车科技适用）", "4-发票付款方（与发票一致）", "5-投保人名称", "6-保单单号", "7-出单保险公司（明细至保险公司分支机构）", "8-险种名称", "9-保单出单日期", "10-全保费", "11-净保费", "12-手续费等级（对应点位台账）", "13-手续费率", "14-手续费总额（报行内+报行外）(含税)", "15-手续费总额（报行内+报行外）(不含税)", "16-收入入账月度", "17-凭证号", "18-手续费比例", "19-手续费金额（含税）", "20-手续费金额（不含税）", "21-回款月度", "22-凭证号", "23-收款金额", "24-开票单位", "25-开票日期", "26-手续费比例", "27-开票金额（不含税）", "28-开票金额（含税）", "29-20191231应收账款（含已开票和未开票）", "30-开票日期", "31-应收回款月度", "32-收款凭证号", "33-收款金额", "34-开票单位", "35-开票日期", "36-开票金额（不含税）", "37-开票金额（含税）", "38-尚未开票金额（不含税）", "39-尚未开票金额（含税）", "40-代理人名称", "41-佣金比例", "42-佣金金额（已入账）", "43-支付主体", "44-支付比例", "45-支付金额", "46-未计提佣金（19年底尚未入帐）", "保险公司", "省", "市", "保险公司id", "handle_sign", "sum_fee", "sum_commission", "gross_profit"]

    @Test
    void step3_2_1() {
        def rows = baseSql.rows("select b.name as '业务名称',a.* from commission_all_2 a,table_type b where a.type_id=b.id and a.flag=1")
        if (rows.size() > 0) {
            log.info("科技付佣-{}", rows.size())
            FileUtils.moveFile(ExcelUtil2.writeToExcel(head2, rows), new File("科技付佣配数据.xlsx"))
        }
    }

    @Test
    void step3_3() {
        def r2 = baseSql.rows("select b.name as 业务,if(a.flag=1,'保代匹配',if(a.flag=2,'科技匹配','未匹配')) as 类型,count(9) as 数量,sum(sum_commission) as 金额 from commission_all_2 a left join table_type b on a.type_id=b.id group by a.flag,b.name" as String)
        ExcelUtil2.writeToExcel(['业务', '类型', '数量', '金额'], r2).renameTo(new File("科技付佣匹配统计.xlsx"))
    }

    // 匹配总部计算数据
    @Test
    void step4() {
        def rows = baseSql.rows("select `id`,`type` from table_type where org='保代-广管'")
        rows.each { type ->
            baseSql.executeUpdate("update settlement_baodai_2 a,commission_${type.type} b set a.flag=1,a.type_id=${type.id} where a.`6-保单单号`=b.`6-保单单号` and a.`8-险种名称`=b.`8-险种名称` and a.flag=0" as String)
            baseSql.executeUpdate("update settlement_baodai_2 a,settlement_${type.type} b set a.flag=1,a.type_id=${type.id} where a.`6-保单单号`=b.`6-保单单号` and a.`8-险种名称`=b.`8-险种名称` and a.flag=0" as String)
        }
    }

    @Test
    void step4_1() {
        def rows = baseSql.rows("select `id`,`type` from table_type where org='保代-广管'")
        rows.each { type ->
            baseSql.executeUpdate("update settlement_baodai_2 a,commission_${type.type} b set a.flag=1,a.type_id=${type.id},a.`8-险种名称`=b.`8-险种名称` where a.`6-保单单号`=b.`6-保单单号` and a.flag=0" as String)
            baseSql.executeUpdate("update settlement_baodai_2 a,settlement_${type.type} b set a.flag=1,a.type_id=${type.id},a.`8-险种名称`=b.`8-险种名称` where a.`6-保单单号`=b.`6-保单单号` and a.flag=0" as String)
        }
    }

    // 导出未匹配总部计算数据
    @Test
    void step4_2() {
        def rows = baseSql.rows("select * from settlement_baodai_2 where flag=0")
        if (rows.size() > 0) {
            FileUtils.moveFile(ExcelUtil2.writeToExcel(head, rows), new File("总部结算.xlsx"))
        }
    }

    @Test
    void step4_3() {
        def r2 = baseSql.rows("select b.name as 业务,if(a.flag=1,'保代匹配',if(a.flag=2,'科技匹配','未匹配')) as 类型,count(9) as 数量,sum(sum_fee) as 金额 from settlement_baodai_2 a left join table_type b on a.type_id=b.id group by a.flag,b.name" as String)
        ExcelUtil2.writeToExcel(['业务', '类型', '数量', '金额'], r2).renameTo(new File("保代结算匹配统计.xlsx"))
    }


    // 匹配sbt结算数据
    @Test
    void step5() {
        def rows = baseSql.rows("select `id`,`type` from table_type where org='保代-广管' or id in (15,31,3)")
        rows.each { type ->
            baseSql.executeUpdate("update settlement_sbt_czl_2 a,commission_${type.type} b set a.flag=1,a.type_id=${type.id} where a.`6-保单单号`=b.`6-保单单号` and a.`8-险种名称`=b.`8-险种名称` and a.flag=0" as String)
            baseSql.executeUpdate("update settlement_sbt_czl_2 a,settlement_${type.type} b set a.flag=1,a.type_id=${type.id} where a.`6-保单单号`=b.`6-保单单号` and a.`8-险种名称`=b.`8-险种名称` and a.flag=0" as String)
        }
    }

    @Test
    void step5_1() {
        def rows = baseSql.rows("select `id`,`type` from table_type where org='保代-广管' or id in (15,31,3)")
        rows.each { type ->
            baseSql.executeUpdate("update settlement_sbt_czl_2 a,commission_${type.type} b set a.flag=1,a.type_id=${type.id},a.`8-险种名称`=b.`8-险种名称` where a.`6-保单单号`=b.`6-保单单号` and a.flag=0" as String)
            baseSql.executeUpdate("update settlement_sbt_czl_2 a,settlement_${type.type} b set a.flag=1,a.type_id=${type.id},a.`8-险种名称`=b.`8-险种名称` where a.`6-保单单号`=b.`6-保单单号` and a.flag=0" as String)
        }
    }

    // 导出未匹配总部计算数据
    @Test
    void step5_2() {
        def rows = baseSql.rows("select * from settlement_sbt_czl_2 where flag=0")
        if (rows.size() > 0) {
            FileUtils.moveFile(ExcelUtil2.writeToExcel(head, rows), new File("总部结算.xlsx"))
        }
    }

    @Test
    void step5_3() {
        def r2 = baseSql.rows("select b.name as 业务,if(a.flag=1,'保代匹配',if(a.flag=2,'科技匹配','未匹配')) as 类型,count(9) as 数量,sum(sum_fee) as 金额 from settlement_sbt_czl_2 a left join table_type b on a.type_id=b.id group by a.flag,b.name" as String)
        ExcelUtil2.writeToExcel(['业务', '类型', '数量', '金额'], r2).renameTo(new File("SBT结算匹配统计.xlsx"))
    }

    // 插入总部数据
    @Test
    void insert1() {
        String sql = '''
insert into commission_zongbu (source_file,c_id, `1-序号`, `2-保代机构`, `3-出单保险代理机构（车车科技适用）`, `4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,`保险公司id`,`保险公司`,`省`,`市`)
select 'commission_zongbu_2',id, `1-序号`, `2-保代机构`, `3-出单保险代理机构（车车科技适用）`, `4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`45-支付金额`,`46-未计提佣金（19年底尚未入帐）`,`保险公司id`,`保险公司`,`省`,`市` from commission_zongbu_2 where flag=1;
'''
        baseSql.executeInsert(sql)
    }

    // 插入超自律数据
    @Test
    void insert2() {
        List<String> types = ["anhui", "dongguan", "foshan", "fujian", "guangdong", "guangxi", "jiangsu", "shandong", "zhejiang", "liaoning", "sichuan"]
        String sql1 = '''
insert into settlement_# (source_file,c_id, `1-序号`, `2-保代机构`, `3-出单保险代理机构（车车科技适用）`, `4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`24-开票单位`,`25-开票日期`,`26-手续费比例`,`27-开票金额（不含税）`,`28-开票金额（含税）`,`34-开票单位`,`35-开票日期`,`36-开票金额（不含税）`,`37-开票金额（含税）`,`保险公司id`,`保险公司`,`省`,`市`)
select 'settlement_#_2',id, `1-序号`, `2-保代机构`, `3-出单保险代理机构（车车科技适用）`, `4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`24-开票单位`,`25-开票日期`,`26-手续费比例`,`27-开票金额（不含税）`,`28-开票金额（含税）`,`34-开票单位`,`35-开票日期`,`36-开票金额（不含税）`,`37-开票金额（含税）`,`保险公司id`,`保险公司`,`省`,`市` from settlement_#_2 where flag=1
'''
        String sql2 = '''
insert into commission_# (source_file,c_id, `1-序号`, `2-保代机构`, `3-出单保险代理机构（车车科技适用）`, `4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`45-支付金额`,`保险公司id`,`保险公司`,`省`,`市`)
select 'commission_#_2',id, `1-序号`, `2-保代机构`, `3-出单保险代理机构（车车科技适用）`, `4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`45-支付金额`,`保险公司id`,`保险公司`,`省`,`市` from commission_#_2 where flag=1
'''
        types.each {
            baseSql.executeInsert(sql1.replace("#", it))
            baseSql.executeInsert(sql2.replace("#", it))
        }
    }

    // 科技付佣
    @Test
    void insert3() {
        String sql = '''
insert into commission_#1 (source_file,c_id, `1-序号`, `2-保代机构`, `3-出单保险代理机构（车车科技适用）`, `4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`45-支付金额`,`保险公司id`,`保险公司`,`省`,`市`)
select 'commission_all_2',id, `1-序号`, `2-保代机构`, `3-出单保险代理机构（车车科技适用）`, `4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`45-支付金额`,`保险公司id`,`保险公司`,`省`,`市` from commission_all_2 where flag=1 and type_id=#2
'''
        def rows = baseSql.rows("select `id`,`type` from table_type where org='保代-广管'")
        rows.each {
            baseSql.executeInsert(sql.replace("#1", it.type).replace("#2", it.id as String))
        }
    }

    // 总部结算
    @Test
    void insert4() {
        String sql = '''
insert into settlement_#1 (source_file,s_id, `1-序号`, `2-保代机构`, `3-出单保险代理机构（车车科技适用）`, `4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`24-开票单位`,`25-开票日期`,`26-手续费比例`,`27-开票金额（不含税）`,`28-开票金额（含税）`,`34-开票单位`,`35-开票日期`,`36-开票金额（不含税）`,`37-开票金额（含税）`,`保险公司id`,`保险公司`,`省`,`市`)
select 'settlement_baodai_2',id, `1-序号`, `2-保代机构`, `3-出单保险代理机构（车车科技适用）`, `4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`24-开票单位`,`25-开票日期`,`26-手续费比例`,`27-开票金额（不含税）`,`28-开票金额（含税）`,`34-开票单位`,`35-开票日期`,`36-开票金额（不含税）`,`37-开票金额（含税）`,`保险公司id`,`保险公司`,`省`,`市` from settlement_baodai_2 where flag=1 and type_id=#2
'''
        def rows = baseSql.rows("select `id`,`type` from table_type where org='保代-广管'")
        rows.each {
            baseSql.executeInsert(sql.replace("#1", it.type).replace("#2", it.id as String))
        }
    }

    // 总部结算
    @Test
    void insert5() {
        String sql = '''
insert into settlement_#1 (source_file,s_id, `1-序号`, `2-保代机构`, `3-出单保险代理机构（车车科技适用）`, `4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`24-开票单位`,`25-开票日期`,`26-手续费比例`,`27-开票金额（不含税）`,`28-开票金额（含税）`,`34-开票单位`,`35-开票日期`,`36-开票金额（不含税）`,`37-开票金额（含税）`,`保险公司id`,`保险公司`,`省`,`市`)
select 'settlement_sbt_czl_2',id, `1-序号`, `2-保代机构`, `3-出单保险代理机构（车车科技适用）`, `4-发票付款方（与发票一致）`,`5-投保人名称`,`6-保单单号`,`7-出单保险公司（明细至保险公司分支机构）`,`8-险种名称`,`9-保单出单日期`,`10-全保费`,`11-净保费`,`24-开票单位`,`25-开票日期`,`26-手续费比例`,`27-开票金额（不含税）`,`28-开票金额（含税）`,`34-开票单位`,`35-开票日期`,`36-开票金额（不含税）`,`37-开票金额（含税）`,`保险公司id`,`保险公司`,`省`,`市` from settlement_sbt_czl_2 where flag=1 and type_id=#2
'''
        def rows = baseSql.rows("select `id`,`type` from table_type where org='保代-广管'")
        rows.each {
            baseSql.executeInsert(sql.replace("#1", it.type).replace("#2", it.id as String))
        }
    }

    @Test
    void clean1() {
        println baseSql.executeUpdate("delete from commission_zongbu where source_file='commission_zongbu_2'")
    }

    @Test
    void clean2() {
        List<String> types = ["anhui", "dongguan", "foshan", "fujian", "guangdong", "guangxi", "jiangsu", "shandong", "zhejiang", "liaoning", "sichuan"]
        types.each {
            println baseSql.executeUpdate("delete from settlement_${it} where source_file='settlement_${it}_2'" as String)
            println baseSql.executeUpdate("delete from commission_${it} where source_file='commission_${it}_2'" as String)
        }
    }

    @Test
    void clean3() {
        String sql = "delete from commission_# where source_file='commission_all_2'"
        def rows = baseSql.rows("select `type` from table_type where org='保代-广管'")
        rows.each {
            println baseSql.executeUpdate(sql.replace("#", it.type))
        }
    }

    @Test
    void clean4() {
        String sql = "delete from settlement_# where source_file='settlement_baodai_2'"
        def rows = baseSql.rows("select `type` from table_type where org='保代-广管'")
        rows.each {
            println baseSql.executeUpdate(sql.replace("#", it.type))
        }
    }

    @Test
    void clean5() {
        String sql = "delete from settlement_# where source_file='settlement_sbt_czl_2'"
        def rows = baseSql.rows("select `type` from table_type where org='保代-广管'")
        rows.each {
            println baseSql.executeUpdate(sql.replace("#", it.type))
        }
    }

    @Test
    void insert() {
        insert1()
        insert2()
        insert3()
        insert4()
        insert5()
    }

    @Test
    void clean() {
        clean1()
        clean2()
        clean3()
        clean4()
        clean5()
    }
}
