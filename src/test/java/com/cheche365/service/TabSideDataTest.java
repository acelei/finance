package com.cheche365.service;

import app.SpringApplicationLauncher;
import lombok.extern.log4j.Log4j2;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringApplicationLauncher.class)
@Log4j2
public class TabSideDataTest {
    @Autowired
    private TabSideData tabSideData;

    @Test
    public void tabSettlement() {
        tabSideData.tabSettlement("yx");
    }

    @Test
    public void t() {
        tabSideData.putDownFlag1("shandong");
    }
}