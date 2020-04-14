package com.cheche365.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;

@Service
public class DataRunService {
    @Autowired
    private InitData initData;
    @Autowired
    private FixInsuranceCompanyArea fixInsuranceCompanyArea;
    @Autowired
    private DataCombine dataCombine;
    @Autowired
    private TabSideData tabSideData;
    @Autowired
    private FixProfit fixProfit;
    @Autowired
    private ResultService resultService;
    @Autowired
    private MatchResultSideData matchResultSideData;
    @Autowired
    private ReMatchResultSideData reMatchResultSideData;
    @Autowired
    private ReMatchSideData reMatchSideData;
    @Autowired
    private MatchSingleData matchSingleData;
    @Autowired
    private ReplaceBusinessData replaceBusinessData;

    /**
     * 导出数据后进行初始化
     * 删除无效数据,处理保单号,险种,保险公司,地区
     *
     * @param type
     */
    public void init(String type) {
        // 合计收入成本
        initData.sumSettlementCommission(type);
        // 删除无效数据
        initData.deleteNullData(type);
        // 处理保单号
        initData.fixPolicyNo(type);
        // 处理险种
        initData.fixInsuranceType(type);
        // 处理保险公司及地区
        fixInsuranceCompanyArea.run(type);
        // 写入result表
        dataCombine.result(type);
        // 写入result2表
        dataCombine.result2(type);
        // 标记result2单边数据
        tabSideData.setDefaultSideFlag(type);
        // 写入back表
        initData.back(type);
        // 写入result3表
        initData.result3(type);
        // 调整保费
        initData.fixPremium(type, "");
    }

    /**
     * 对数据进行处理
     *
     * @param type
     */
    public void process(String type) {
        // 1.调整单边收入保费异常数据
        tabSideData.tabSettlement(type);
        tabSideData.tabCommission(type);
        // 2.result中进行单边数据匹配
        matchResultSideData.run(type);
        // 3.对毛利率问题数据进行调整
        fixProfit.fixSettlementCommission(type);
        // 4.result中毛利率异常匹配自身单边数据
        matchSingleData.matchSingleDataList(type, true);
        // 5.在result2中将单边数据匹配毛利率较高数据
        reMatchResultSideData.run(type);
        // 6.将剩下的单边数据标记下放
        tabSideData.putDownFlag(type);
        // 7.将result_2中的毛利率异常数据匹配散表中的单边数据
        matchSingleData.matchSingleDataList(type, false);
        // 8.将settlement,commission剩余单边数据匹配至result2中毛利率较高数据
        reMatchSideData.run(type);
        // 9.设置替换标记
        initData.flagErrData(type);
        // 10.保费与收入数据比例异常的数据做数据替换
        replaceBusinessData.replaceBusinessList(type);
    }

    /**
     * 对替换后数据进行合并
     *
     * @param type
     */
    public void result(String type) throws SQLException {

        resultService.run(type);
    }

    public void reRun(String type) {
        // 恢复result2表数据
        initData.roll(type);
        // 调整保费
        initData.fixPremium(type, "");
        process(type);
    }
}
