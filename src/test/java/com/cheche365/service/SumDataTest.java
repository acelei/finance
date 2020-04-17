package com.cheche365.service;

import app.SpringApplicationLauncher;
import com.cheche365.util.ExcelUtil2;
import com.google.common.collect.Lists;
import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringApplicationLauncher.class)
@Log4j2
public class SumDataTest {
    @Autowired
    Sql baseSql;
    @Autowired
    SumData sumData;

    @Test
    public void sumResult3() throws SQLException {
        List<GroovyRowResult> rows = baseSql.rows("select id,`type` from table_type where org!='科技'");
        baseSql.execute("truncate result_sum_data_3");
        for (GroovyRowResult row : rows) {
            Integer id = MapUtils.getInteger(row, "id");
            String type = MapUtils.getString(row, "type");
            sumData.sumResult3(type, id);
        }
    }

    String tjSql = "select org,name,responsible,`3-出单保险代理机构（车车科技适用）`,`7-出单保险公司（明细至保险公司分支机构）`,\n" +
            "       sum(`14-手续费总额（报行内+报行外）(含税)`) as `14-手续费总额（报行内+报行外）(含税)`,\n" +
            "       sum(`15-手续费总额（报行内+报行外）(不含税)`) as `15-手续费总额（报行内+报行外）(不含税)`,\n" +
            "       sum(`19-手续费金额（含税）`) as `19-手续费金额（含税）`,\n" +
            "       sum(`20-手续费金额（不含税）`) as `20-手续费金额（不含税）`,\n" +
            "       sum(`23-收款金额`) as `23-收款金额`,\n" +
            "       sum(`27-开票金额（不含税）`) as `27-开票金额（不含税）`,\n" +
            "       sum(`28-开票金额（含税）`) as `28-开票金额（含税）`,\n" +
            "       sum(`29-20191231应收账款（含已开票和未开票）`) as `29-20191231应收账款（含已开票和未开票）`,\n" +
            "       sum(`33-收款金额`) as `33-收款金额`,\n" +
            "       sum(`37-开票金额（含税）`) as `37-开票金额（含税）`,\n" +
            "       sum(`38-尚未开票金额（不含税）`) as `38-尚未开票金额（不含税）`,\n" +
            "       sum(`39-尚未开票金额（含税）`) as `39-尚未开票金额（含税）`,\n" +
            "       sum(`42-佣金金额（已入账）`) as `42-佣金金额（已入账）`,\n" +
            "       sum(`45-支付金额`) as `45-支付金额`,\n" +
            "       sum(`46-未计提佣金（19年底尚未入帐）`) as `46-未计提佣金（19年底尚未入帐）`\n" +
            "       from result_sum_data_3 a,table_type b where a.type_id=b.id group by type_id,`3-出单保险代理机构（车车科技适用）`,`7-出单保险公司（明细至保险公司分支机构）`";

    @Test
    public void exportFile() throws SQLException, IOException {
        List<String> head = Lists.newArrayList(
                "org",
                "name",
                "responsible",
                "3-出单保险代理机构（车车科技适用）",
                "7-出单保险公司（明细至保险公司分支机构）",
                "14-手续费总额（报行内+报行外）(含税)",
                "15-手续费总额（报行内+报行外）(不含税)",
                "19-手续费金额（含税）",
                "20-手续费金额（不含税）",
                "23-收款金额",
                "27-开票金额（不含税）",
                "28-开票金额（含税）",
                "29-20191231应收账款（含已开票和未开票）",
                "33-收款金额",
                "37-开票金额（含税）",
                "38-尚未开票金额（不含税）",
                "39-尚未开票金额（含税）",
                "42-佣金金额（已入账）",
                "45-支付金额",
                "46-未计提佣金（19年底尚未入帐）"
        );
        List<GroovyRowResult> rows = baseSql.rows(tjSql);
        List<Map> list = Lists.newArrayList(rows);
        File file = ExcelUtil2.writeToExcel(head, list);
        FileUtils.copyFile(file, new File("统计_保代.xlsx"));
    }

    String errTjSql = "select ':type' as '业务名称', '整合表' as '数据表','毛利异常数据' as '描述',count(9) as '条目数',sum(sum_fee) '收入',sum(sum_commission) as '成本' from result_#_back where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and abs(gross_profit)>1\n" +
            "union all\n" +
            "select ':type','整合表','无法处理的毛利异常数据',count(9),sum(sum_fee),sum(sum_commission) from result_#_2 where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and handle_sign=2\n" +
            "union all\n" +
            "select ':type','整合表','需要替换整合数据',count(9),sum(sum_fee),sum(sum_commission) from result_#_2 where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and handle_sign in (3,9)\n" +
            "union all\n" +
            "select ':type','整合表','未替换整合数据',count(9),sum(sum_fee),sum(sum_commission) from result_#_2 where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and handle_sign=3\n" +
            "union all\n" +
            "select ':type','整合表','已替换整合数据',count(9),sum(sum_fee),sum(sum_commission) from result_#_2 where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and handle_sign=9\n" +
            "union all\n" +
            "select ':type','收入表','需要替换收入数据',count(9),sum(sum_fee),sum(sum_commission) from settlement_# where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and handle_sign in (6,9)\n" +
            "union all\n" +
            "select ':type','收入表','未替换收入数据',count(9),sum(sum_fee),sum(sum_commission) from settlement_# where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and handle_sign=6\n" +
            "union all\n" +
            "select ':type','收入表','已替换收入数据',count(9),sum(sum_fee),sum(sum_commission) from settlement_# where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and handle_sign=9\n" +
            "union all\n" +
            "select ':type','成本表','需要替换成本数据',count(9),sum(sum_fee),sum(sum_commission) from commission_# where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and handle_sign in (6,9)\n" +
            "union all\n" +
            "select ':type','成本表','未替换成本数据',count(9),sum(sum_fee),sum(sum_commission) from commission_# where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and handle_sign=6\n" +
            "union all\n" +
            "select ':type','成本表','已替换成本数据',count(9),sum(sum_fee),sum(sum_commission) from commission_# where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and handle_sign=9";


    @Test
    public void exportErrTj() throws SQLException, IOException, InterruptedException {
        List<String> head = Lists.newArrayList(
                "业务名称",
                "数据表",
                "描述",
                "条目数",
                "收入",
                "成本"
        );
        List<GroovyRowResult> rows = baseSql.rows("select `name`,`type` from table_type where flag=3");
        List<Map> list = new ArrayList<>();
        for (GroovyRowResult row : rows) {
            String typeName = MapUtils.getString(row, "name");
            String type = MapUtils.getString(row, "type");
            List<GroovyRowResult> dataList = baseSql.rows(errTjSql.replace(":type", typeName).replace("#", type));
            list.addAll(dataList);
        }

        File file = ExcelUtil2.writeToExcel(head, list);
        FileUtils.copyFile(file, new File("错误统计.xlsx"));
    }
}