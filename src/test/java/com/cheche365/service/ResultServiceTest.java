package com.cheche365.service;

import app.SpringApplicationLauncher;
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

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringApplicationLauncher.class)
@Log4j2
public class ResultServiceTest {
    @Autowired
    private ResultService resultService;

    @Test
    public void run() throws SQLException {
        resultService.run("bj");
    }

    @Test
    public void export() throws IOException {
        FileUtils.moveFile(resultService.exportResult("bj"), new File("bj.zip"));
    }
}