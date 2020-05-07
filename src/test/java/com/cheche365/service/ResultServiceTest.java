package com.cheche365.service;

import app.SpringApplicationLauncher;
import com.cheche365.util.ExcelUtil2;
import com.cheche365.util.ThreadPool;
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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutionException;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringApplicationLauncher.class)
@Log4j2
public class ResultServiceTest {
    @Autowired
    private ResultService resultService;
    @Autowired
    private Sql baseSql;
    @Autowired
    private ThreadPool taskThreadPool;

    @Test
    public void run() throws SQLException {
        resultService.run("sbt");
    }

    @Test
    public void result2() throws SQLException {
        List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where flag=5");

        for (GroovyRowResult row : rows) {
            resultService.fix(row.get("type").toString());
        }

    }

    @Test
    public void export() throws IOException, SQLException, ExecutionException, InterruptedException {
        List<GroovyRowResult> rows = baseSql.rows("select `type`,`name` from table_type where flag=5 and org!='科技' limit 1");

        List<File> fileList = taskThreadPool.submitWithResult(rows, row -> {
            String t = row.get("type").toString();
            String name = row.get("name").toString();
            String day = LocalDate.now().format(DateTimeFormatter.ofPattern("MMdd"));
            File f = new File("tmp/2019审计台账-" + name + "(" + day + ").xlsx");
            return resultService.exportResult(t, f);
        });

        File file = ExcelUtil2.zipFiles(fileList, null);
        FileUtils.moveFile(file, new File("车车保代(0505).zip"));
    }
}