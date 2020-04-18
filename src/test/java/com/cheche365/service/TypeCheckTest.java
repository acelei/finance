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
public class TypeCheckTest {
    @Autowired
    private TabSideData tabSideData;
    @Autowired
    private MatchResultSideData matchResultSideData;
    @Autowired
    private FixProfit fixProfit;
    @Autowired
    private ReMatchResultSideData reMatchResultSideData;
    @Autowired
    private ReMatchSideData reMatchSideData;
    @Autowired
    private InitData initData;
    @Autowired
    private ResultService resultService;

    String type = "bj";

    @Test
    public void before() {
        initData.roll(type);
        initData.fixPremium(type, "");
        tabSideData.tabSettlement(type);
        tabSideData.tabCommission(type);
        matchResultSideData.run(type);
        fixProfit.fixSettlementCommission(type);
    }

    @Test
    public void type1() {
        tabSideData.tabSettlement(type);
        tabSideData.tabCommission(type);
    }

    @Test
    public void type2() {
        matchResultSideData.run(type);
    }

    @Test
    public void type3() {
        fixProfit.fixSettlementCommission(type);
    }

    @Test
    public void type5() {
        reMatchResultSideData.run(type);
    }

    @Test
    public void after() {
        tabSideData.putDownFlag2(type);
    }

    @Test
    public void type8() {
        reMatchSideData.run(type);
    }

    @Test
    public void type9() {
        initData.flagErrData(type);
    }

    @Test
    public void end() {
        resultService.run(type);
    }
}
