package com.cheche365.service;

import app.SpringApplicationLauncher;
import com.cheche365.util.ThreadPool;
import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import lombok.extern.log4j.Log4j2;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.SQLException;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringApplicationLauncher.class)
@Log4j2
public class DataCombineTest {
    @Autowired
    private DataCombine dataCombine;
    @Autowired
    private InitData initData;
    @Autowired
    private Sql baseSql;
    @Autowired
    private ThreadPool runThreadPool;

    /**
     * 匹配生成result,result2及back
     * 同时修正result2保费
     *
     * @throws SQLException
     * @throws InterruptedException
     */
    @Test
    public void run() throws SQLException, InterruptedException {
        List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where flag=1");

        runThreadPool.executeWithLatch(rows, (it) -> {
            String type = it.get("type").toString();
            // 写入result表
            dataCombine.result(type);
            // 写入result2表
            dataCombine.result2(type);
            // 写入back表
            initData.back(type);
            // 调整保费
            initData.fixPremium(type, "result_#_2");
        }).await();
    }


    @Test
    public void signRun() {
        String type = "qinqi";
        // 写入result表
        dataCombine.result(type);
        // 写入result2表
        dataCombine.result2(type);
        // 写入back表
        initData.back(type);
        // 调整保费
        initData.fixPremium(type, "result_#_2");
    }

    /**
     * 匹配生成result
     */
    @Test
    public void result() {
        dataCombine.result("");
    }
}