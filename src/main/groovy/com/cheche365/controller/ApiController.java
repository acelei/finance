package com.cheche365.controller;

import com.cheche365.entity.RestResponse;
import com.cheche365.service.*;
import com.cheche365.util.ExcelUtil2;
import com.cheche365.util.ThreadPoolUtils;
import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import lombok.extern.log4j.Log4j2;
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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
    private ReMatchSideData reMatchSideData;
    @Autowired
    private ResultService resultService;

    @GetMapping({"data/before/{type}", "data/before"})
    public RestResponse<String> before(@PathVariable(required = false) String type) throws SQLException {
        if (type != null) {
            initData.run(type);
        } else {
            List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where flag=0");

            for (GroovyRowResult row : rows) {
                ThreadPoolUtils.getTaskPool().execute(() -> {
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
                ThreadPoolUtils.getTaskPool().execute(() -> {
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
                ThreadPoolUtils.getTaskPool().execute(() -> {
                    String t = row.get("type").toString();
                    try {
                        dataRunService.process(t);
                        baseSql.executeUpdate("update table_type set flag=3 where `type`=?", new Object[]{t});
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
                ThreadPoolUtils.getTaskPool().execute(() -> {
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

    @GetMapping({"data/result/{type}", "data/result"})
    public RestResponse<String> result(@PathVariable(required = false) String type) throws SQLException {
        if (type != null) {
            dataRunService.result(type);
        } else {
            List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where flag=4");

            for (GroovyRowResult row : rows) {
                ThreadPoolUtils.getTaskPool().execute(() -> {
                    String t = row.get("type").toString();
                    try {
                        dataRunService.result(t);
                        baseSql.executeUpdate("update table_type set flag=5 where `type`=?", new Object[]{t});
                    } catch (SQLException e) {
                        log.error("结果输出错误:" + t, e);
                    }
                });
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

    @GetMapping("data/fixRef/{type}")
    public RestResponse<String> fixRef(@PathVariable String type) {
        initData.fixRef(type);
        return RestResponse.success(type);
    }

    @GetMapping("data/reMatch/{type}")
    public RestResponse<String> reMatch(@PathVariable String type) {
        reMatchSideData.run(type);
        return RestResponse.success(type);
    }

    @GetMapping({"data/exportResult/{type}", "data/exportResult"})
    public ResponseEntity exportResult(@PathVariable(required = false) String type) throws SQLException, ExecutionException, InterruptedException, IOException {
        File file = null;
        try {
            if (type != null) {
                file = resultService.exportResult(type);
            } else {
                List<GroovyRowResult> rows = baseSql.rows("select `type`,`name` from table_type where flag=5");

                List<Future<File>> futureList = ThreadPoolUtils.submitRun(rows, row -> {
                            String t = row.get("type").toString();
                            String name = row.get("name").toString();
                            File f = new File(name + ".zip");
                            return resultService.exportResult(t, f);
                        }
                );

                List<File> fileList = new ArrayList<>();
                for (Future<File> future : futureList) {
                    fileList.add(future.get());
                }
                file = ExcelUtil2.zipFiles(fileList, null);
                type = "all";
            }

            return downloadFile("export_#.zip".replace("#", type), file);
        } finally {
            if (file != null) {
                file.delete();
            }
        }
    }

    protected ResponseEntity downloadFile(String fileName, File file) {
        HttpHeaders headers = new HttpHeaders();
        headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
        headers.add("Content-Disposition", "attachment; filename=" + fileName + "; filename*=utf-8''" + fileName);
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
