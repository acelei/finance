package com.cheche365.service;

import app.SpringApplicationLauncher;
import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import lombok.extern.log4j.Log4j2;
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

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringApplicationLauncher.class)
@Log4j2
public class ResultServiceTest {
    @Autowired
    private ResultService resultService;
    @Autowired
    private Sql baseSql;

    @Test
    public void run() throws SQLException {
        resultService.run("bj");
    }

    @Test
    public void result2() throws SQLException {
        List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where flag=5");

        for (GroovyRowResult row : rows) {
            resultService.result2(row.get("type").toString());
        }

    }

    @Test
    public void export() throws IOException {
        FileUtils.moveFile(resultService.exportResult("bj"), new File("bj.zip"));
    }
}