package com.cheche365.controller;

import com.cheche365.entity.RestResponse;
import com.cheche365.service.*;
import com.cheche365.util.ExcelUtil2;
import com.cheche365.util.ThreadPool;
import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutionException;

@RestController
@Log4j2
public class ApiController {
    @Autowired
    private Sql baseSql;
    @Autowired
    private DataRunService dataRunService;
    @Autowired
    private InitData initData;
    @Autowired
    private ReplaceBusinessData replaceBusinessData;
    @Autowired
    private ResultService resultService;
    @Autowired
    private SumData sumData;
    @Autowired
    private ReplaceHisBusiness replaceHisBusiness;
    @Autowired
    private ThreadPool taskThreadPool;

    @GetMapping({"data/before/{type}", "data/before"})
    public RestResponse<String> before(@PathVariable(required = false) String type) throws SQLException {
        if (type != null) {
            initData.run(type);
        } else {
            List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where flag=0");

            for (GroovyRowResult row : rows) {
                taskThreadPool.getPool().execute(() -> {
                    String t = row.get("type").toString();
                    try {
                        initData.run(t);
                        baseSql.executeUpdate("update table_type set flag=1 where `type`=?", new Object[]{t});
                    } catch (Exception e) {
                        log.error("预处理错误:" + t, e);
                    }
                });
            }
        }

        return RestResponse.success(type);
    }

    @GetMapping({"data/init/{type}", "data/init"})
    public RestResponse<String> init(@PathVariable(required = false) String type) throws SQLException {
        if (type != null) {
            dataRunService.init(type);
        } else {
            List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where flag=1");

            for (GroovyRowResult row : rows) {
                taskThreadPool.getPool().execute(() -> {
                    String t = row.get("type").toString();
                    try {
                        dataRunService.init(t);
                        baseSql.executeUpdate("update table_type set flag=2 where `type`=?", new Object[]{t});
                    } catch (Exception e) {
                        log.error("初始化错误:" + t, e);
                    }
                });
            }
        }
        return RestResponse.success(type);
    }

    @GetMapping({"data/process/{type}", "data/process"})
    public RestResponse<String> process(@PathVariable(required = false) String type) throws SQLException {
        if (type != null) {
            dataRunService.process(type);
        } else {
            List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where flag=2");

            for (GroovyRowResult row : rows) {
                taskThreadPool.getPool().execute(() -> {
                    String t = row.get("type").toString();
                    try {
                        dataRunService.process(t);
                        baseSql.executeUpdate("update table_type set flag=3 where `type`=?", new Object[]{t});
                        replaceBusinessData.replaceBusinessList(t);
                        baseSql.executeUpdate("update table_type set flag=4 where `type`=?", new Object[]{t});
                    } catch (Exception e) {
                        log.error("数据处理错误:" + t, e);
                    }
                });
            }
        }
        return RestResponse.success(type);
    }

    @GetMapping({"data/replace/{type}", "data/replace"})
    public RestResponse<String> replace(@PathVariable(required = false) String type) throws SQLException {
        if (type != null) {
            replaceBusinessData.replaceBusinessList(type);
        } else {
            List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where flag=3");

            for (GroovyRowResult row : rows) {
                taskThreadPool.getPool().execute(() -> {
                    String t = row.get("type").toString();
                    try {
                        replaceBusinessData.replaceBusinessList(t);
                        baseSql.executeUpdate("update table_type set flag=4 where `type`=?", new Object[]{t});
                    } catch (Exception e) {
                        log.error("数据替换错误:" + t, e);
                    }
                });
            }
        }
        return RestResponse.success(type);
    }

    @GetMapping({"data/replaceHis/{type}"})
    public RestResponse<String> replaceHis(@PathVariable String type) throws SQLException {
        replaceHisBusiness.replaceHistoryBusiness(type);
        return RestResponse.success(type);
    }

    @GetMapping({"data/result/{type}", "data/result"})
    public RestResponse<String> result(@PathVariable(required = false) String type) throws SQLException {
        if (type != null) {
            dataRunService.result(type);
        } else {
            List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where flag=4");

            for (GroovyRowResult row : rows) {
                String t = row.get("type").toString();
                try {
                    dataRunService.result(t);
                    baseSql.executeUpdate("update table_type set flag=5 where `type`=?", new Object[]{t});
                } catch (SQLException e) {
                    log.error("结果输出错误:" + t, e);
                }
            }
        }
        return RestResponse.success(type);
    }

    @GetMapping("data/rollback/{type}")
    public RestResponse<String> rollback(@PathVariable String type) {
        initData.roll(type);
        return RestResponse.success(type);
    }

    @GetMapping("data/reRun/{type}")
    public RestResponse<String> reRun(@PathVariable String type) {
        dataRunService.reRun(type);
        return RestResponse.success(type);
    }

    @GetMapping("data/clean/{type}")
    public RestResponse<String> clean(@PathVariable String type) {
        initData.clean(type);
        return RestResponse.success(type);
    }

    @GetMapping("data/fixRef")
    public RestResponse<String> fixRef() throws SQLException {
        List<GroovyRowResult> rows = baseSql.rows("select `id`,`type` from table_type where flag>=3");
        for (GroovyRowResult row : rows) {
            String t = row.get("type").toString();
            String id = row.get("id").toString();
            initData.fixRefType(t,id);
        }

        initData.fixRef();
        return RestResponse.success();
    }

    @GetMapping({"data/reMatch/{type}", "data/reMatch"})
    public RestResponse<String> reMatch(@PathVariable(required = false) String type) throws SQLException {
        if (type != null) {
            dataRunService.reMatch(type);
        } else {
            List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where flag>=3");

            for (GroovyRowResult row : rows) {
                taskThreadPool.getPool().execute(() -> {
                    String t = row.get("type").toString();
                    dataRunService.reMatch(t);
                });
            }
        }
        return RestResponse.success(type);
    }

    @GetMapping({"data/exportResult/{type}"})
    public ResponseEntity exportResult(@PathVariable String type) throws SQLException, ExecutionException, InterruptedException, IOException {
        File file = resultService.exportResult(type);
        return downloadFile("export_#.zip".replace("#", type), file);
    }

    @GetMapping("data/exportResultKj")
    public ResponseEntity exportResult1(@PathVariable(required = false) String type) throws SQLException, ExecutionException, InterruptedException, IOException {
        List<GroovyRowResult> rows = baseSql.rows("select `type`,`name` from table_type where flag=5 and org='科技'");

        List<File> fileList = taskThreadPool.submitWithResult(rows, row -> {
            String t = row.get("type").toString();
            String name = row.get("name").toString();
            String day = LocalDate.now().format(DateTimeFormatter.ofPattern("MMdd"));
            File f = new File("tmp/2019审计台账-" + name + "(" + day + ").xlsx");
            return resultService.exportResult(t, f);
        });

        File file = ExcelUtil2.zipFiles(fileList, null);

        return downloadFile("车车科技.zip", file);
    }

    @GetMapping("data/exportResultBd")
    public ResponseEntity exportResult2() throws SQLException, ExecutionException, InterruptedException, IOException {
        List<GroovyRowResult> rows = baseSql.rows("select `type`,`name` from table_type where flag=5 and org!='科技'");

        List<File> fileList = taskThreadPool.submitWithResult(rows, row -> {
            String t = row.get("type").toString();
            String name = row.get("name").toString();
            String day = LocalDate.now().format(DateTimeFormatter.ofPattern("MMdd"));
            File f = new File("tmp/2019审计台账-" + name + "(" + day + ").xlsx");
            return resultService.exportResult(t, f);
        });

        File file = ExcelUtil2.zipFiles(fileList, null);

        return downloadFile("车车保代.zip", file);
    }

    @GetMapping({"data/tj/{type}", "data/tj"})
    public ResponseEntity statistics(@PathVariable(required = false) String type) throws SQLException, ExecutionException, InterruptedException, IOException {
        File file = null;
        if (type != null) {
            GroovyRowResult row = baseSql.firstRow("select `type`,`name` from table_type where type=?", new String[]{type});
            file = sumData.statistics(MapUtils.getString(row, "type"), MapUtils.getString(row, "name"));
        } else {

            file = sumData.statisticsAll("select `type`,`name` from table_type where flag>=3");
            type = "all";
        }
        return downloadFile("统计_#.xlsx".replace("#", type), file);
    }

    @GetMapping("data/delTmp")
    public RestResponse<String> delTmp() throws IOException {
        FileUtils.deleteDirectory(ExcelUtil2.tmp);
        FileUtils.forceMkdir(ExcelUtil2.tmp);
        return RestResponse.success();
    }

    protected ResponseEntity downloadFile(String fileName, File file) {
        String tmpFileName = new String(fileName.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1);
        HttpHeaders headers = new HttpHeaders();
        headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
        headers.add("Content-Disposition", "attachment; filename=" + tmpFileName + "; filename*=utf-8''" + tmpFileName);
        headers.add("Pragma", "no-cache");
        headers.add("Expires", "0");
        return ResponseEntity
                .ok()
                .headers(headers)
                .contentLength(file.length())
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(new FileSystemResource(file));
    }
}
