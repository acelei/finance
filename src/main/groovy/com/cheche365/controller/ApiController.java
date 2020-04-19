package com.cheche365.controller;

import com.cheche365.entity.RestResponse;
import com.cheche365.service.DataRunService;
import com.cheche365.service.InitData;
import com.cheche365.service.ReplaceBusinessData;
import com.cheche365.util.ThreadPoolUtils;
import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.util.List;

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
                        log.error("预处理错误", e);
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
                        log.error("初始化错误", e);
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
                        log.error("数据处理错误", e);
                    }
                });
            }
        }
        return RestResponse.success(type);
    }

    @GetMapping({"data/replace/{type}", "data/replace"})
    public RestResponse<String> replace(@PathVariable String type) throws SQLException {
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
                        log.error("数据替换错误", e);
                    }
                });
            }
        }
        return RestResponse.success(type);
    }

    @GetMapping({"data/result/{type}", "data/result"})
    public RestResponse<String> result(@PathVariable String type) throws SQLException {
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
                        log.error("结果输出错误", e);
                    }
                });
            }
        }
        return RestResponse.success(type);
    }

    @GetMapping("data/rollback/{type}")
    public RestResponse<String> rollback(@PathVariable String type) {
        initData.roll(type);
        initData.fixPremium(type, "");
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
}
