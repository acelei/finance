package com.cheche365.service;

import app.SpringApplicationLauncher;
import com.cheche365.util.ExcelUtil2;
import com.cheche365.util.ThreadPoolUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringApplicationLauncher.class)
@Log4j2
public class DataRunTest {
    @Autowired
    private DataRunService dataRunService;
    @Autowired
    private FixProfit fixProfit;
    @Autowired
    private InitData initData;
    @Autowired
    private Sql baseSql;
    @Autowired
    private MatchSingleData matchSingleData;
    @Autowired
    private ReplaceBusinessData replaceBusinessData;
    @Autowired
    private InsertTableData insertTableData;

    @Test
    public void init() throws SQLException {
        List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where flag=0");

        for (GroovyRowResult row : rows) {
            String type = row.get("type").toString();
            initData.run(type);
//            dataRunService.init(type);
            baseSql.executeUpdate("update table_type set flag=1 where `type`=?", new Object[]{type});
        }
    }

    // 对数据字段进行检查

    @Test
    public void run() throws SQLException {
        List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where flag=1");

        for (GroovyRowResult it : rows) {
            String type = it.get("type").toString();
            dataRunService.init(type);
            dataRunService.process(type);
            baseSql.executeUpdate("update table_type set flag=2 where `type`=?", new Object[]{type});
        }
    }

    // 对数据进行替换

    @Test
    public void result() throws SQLException {
        List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where flag=2");

        for (GroovyRowResult it : rows) {
            String type = it.get("type").toString();
            replaceBusinessData.replaceBusinessList(type);
            baseSql.executeUpdate("update table_type set flag=3 where `type`=?", new Object[]{type});
        }
    }

    @Test
    public void export() throws SQLException, InterruptedException {
        List<GroovyRowResult> types = baseSql.rows("select `type` from table_type where type in ('sbt')");

        ThreadPoolUtils.executeTask(types, (it) -> {
            String type = it.get("type").toString();

            File file = null;
            try {
                file = exportFile(type, "select * from result_#_3");
            } catch (ExecutionException | InterruptedException | SQLException e) {
                log.error("获取文件失败", e);
            }
            try {
                FileUtils.copyFile(file, new File(type + "_合并2.xlsx"));
            } catch (IOException e) {
                log.error("文件拷贝失败", e);
            }
        }).await();
    }

    @Test
    public void export2() throws SQLException, InterruptedException {
        List<String> area = Lists.newArrayList(
                "北京市",
                "吉林省",
                "四川省",
                "安徽省",
                "山东省",
                "山西省",
                "广东省",
                "广西壮族自治区",
                "江苏省",
                "江西省",
                "河北省",
                "浙江省",
                "湖北省",
                "湖南省",
                "福建省",
                "辽宁省",
                "陕西省"
        );

        ThreadPoolUtils.executeTask(area, (it) -> {

            File file = null;
            try {
                file = exportFile(it, "select * from result_sbt_3 where `省`='#'");
            } catch (ExecutionException | InterruptedException | SQLException e) {
                log.error("获取文件失败", e);
            }
            try {
                FileUtils.copyFile(file, new File(it + "_sbt.xlsx"));
            } catch (IOException e) {
                log.error("文件拷贝失败", e);
            }
        }).await();
    }

    @Test
    public void reRun() throws SQLException {
        List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where flag=2");

        for (GroovyRowResult it : rows) {
            String type = it.get("type").toString();
            dataRunService.reRun(type);
        }
    }

    private String type = "bj";

    @Test
    public void singRun() {
        initData.run(type);
    }

    @Test
    public void signInit() {
        dataRunService.init(type);
    }

    @Test
    public void signRun() {
        dataRunService.process(type);
    }

    @Test
    public void signReRun() {
        dataRunService.reRun(type);
    }

    @Test
    public void replace(){
        replaceBusinessData.replaceBusinessList(type);
    }

    @Test
    public void signResult() throws SQLException {
        dataRunService.result(type);
    }

    @Test
    public void exportSignFile() throws InterruptedException, ExecutionException, SQLException, IOException {
        File f = exportFile(type, "select * from result_sbt_3 where `省` is null");
        FileUtils.copyFile(f, new File("空_sbt.xlsx"));
    }

    @Test
    public void exportRjFile() throws InterruptedException, ExecutionException, SQLException, IOException {
        exportFile("sbt");
    }

    @Test
    public void t() {
        fixProfit.fixSettlementCommission(type);
    }

    public File exportFile(String type, String sql) throws ExecutionException, InterruptedException, SQLException {
        List<String> head = Lists.newArrayList("id", "s_id", "d_id", "c_id", "source_file", "1-序号", "2-保代机构", "3-出单保险代理机构（车车科技适用）", "4-发票付款方（与发票一致）", "5-投保人名称", "6-保单单号", "7-出单保险公司（明细至保险公司分支机构）", "8-险种名称", "9-保单出单日期", "10-全保费", "11-净保费", "12-手续费等级（对应点位台账）", "13-手续费率", "14-手续费总额（报行内+报行外）(含税)", "15-手续费总额（报行内+报行外）(不含税)", "16-收入入账月度", "17-凭证号", "18-手续费比例", "19-手续费金额（含税）", "20-手续费金额（不含税）", "21-回款月度", "22-凭证号", "23-收款金额", "24-开票单位", "25-开票日期", "26-手续费比例", "27-开票金额（不含税）", "28-开票金额（含税）", "29-20191231应收账款（含已开票和未开票）", "30-开票日期", "31-应收回款月度", "32-收款凭证号", "33-收款金额", "34-开票单位", "35-开票日期", "36-开票金额（不含税）", "37-开票金额（含税）", "38-尚未开票金额（不含税）", "39-尚未开票金额（含税）", "40-代理人名称", "41-佣金比例", "42-佣金金额（已入账）", "43-支付主体", "44-支付比例", "45-支付金额", "46-未计提佣金（19年底尚未入帐）", "保险公司", "省", "市", "保险公司id");
        BlockingDeque<Map> dataList = Queues.newLinkedBlockingDeque(10000);
        Future<File> f = ThreadPoolUtils.getRunPool().submit(() -> ExcelUtil2.writeToExcel(head, dataList));

        int index = 0;
        boolean flag = true;
        String querySql = "";
        if (sql.contains("where")) {
            querySql = sql.replace("#", type) + " and id>? limit 20000";
        } else {
            querySql = sql.replace("#", type) + " where id>? limit 20000";
        }


        while (flag) {
            List<GroovyRowResult> rows = baseSql.rows(querySql, new Object[]{index});
            if (rows != null && rows.size() > 0) {
                for (GroovyRowResult row : rows) {
                    dataList.put(row);
                }
                index = MapUtils.getInteger(rows.get(rows.size() - 1), "id");
            } else {
                flag = false;
            }
        }
        dataList.put(ExcelUtil2.EMPTY_MAP);

        return f.get();
    }

    String tjSql = "select `省`,`3-出单保险代理机构（车车科技适用）`,`7-出单保险公司（明细至保险公司分支机构）`,\n" +
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
            "       from result_#_3 group by `省`,`3-出单保险代理机构（车车科技适用）`,`7-出单保险公司（明细至保险公司分支机构）`";

    public void exportFile(String type) throws SQLException, IOException {
        List<String> head = Lists.newArrayList(
                "省",
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
        List<GroovyRowResult> rows = baseSql.rows(tjSql.replace("#", type));
        List<Map> list = Lists.newArrayList(rows);
        File file = ExcelUtil2.writeToExcel(head, list);
        FileUtils.copyFile(file, new File("统计_sbt.xlsx"));
    }

    @Test
    public void testMatchSingleData() {
        matchSingleData.matchSingleDataList("bj", true);
    }

    @Test
    public void testReplaceBusinessData() {
        replaceBusinessData.replaceBusinessList("bj");
    }

}
