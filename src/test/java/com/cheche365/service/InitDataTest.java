package com.cheche365.service;

import app.SpringApplicationLauncher;
import com.cheche365.util.ThreadPoolUtils;
import com.google.common.collect.Lists;
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
public class InitDataTest {
    @Autowired
    private InitData initData;
    @Autowired
    private Sql baseSql;
    @Autowired
    private TabSideData tabSideData;
    @Autowired
    private DataRunService dataRunService;

    /**
     * 删除无效数据
     *
     * @throws SQLException
     */
    @Test
    public void deleteNullData() throws SQLException {
        List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type where flag=0");

        for (GroovyRowResult row : rows) {
            initData.deleteNullData(row.get("type").toString());
        }
    }


    /**
     * 将back数据恢复至result2
     *
     * @throws SQLException
     */
    @Test
    public void roll() throws SQLException {
        List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type");

        for (GroovyRowResult row : rows) {
            String type = row.get("type").toString();
            initData.roll(type);
            baseSql.executeUpdate("update table_type set flag=2 where `type`=?", new Object[]{type});
        }
    }


    String[] types = new String[]{"chongqing", "guangdong", "chengshuo", "henan", "liaoning", "guangxi", "jiangsu"};

    @Test
    public void createTable() throws SQLException {
        String type = "shandong";

        baseSql.execute("create table settlement_# like settlement_bj".replace("#", type));
        baseSql.execute("create table commission_# like commission_bj".replace("#", type));
        baseSql.execute("create table result_# like result_bj".replace("#", type));
        baseSql.execute("create table result_#_2 like result_bj_2".replace("#", type));
        baseSql.execute("create table result_#_back like result_bj_back".replace("#", type));
        baseSql.execute("create table result_#_3 like result_bj_3".replace("#", type));
        baseSql.execute("create table result_#_final like result_bj_final".replace("#", type));
        baseSql.execute("create table result_#_2_final like result_bj_2_final".replace("#", type));

    }

    @Test
    public void modifyTable() throws SQLException {
        List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type");

        for (GroovyRowResult row : rows) {
            String type = row.get("type").toString();
            try {
                baseSql.execute("ALTER TABLE `settlement_#` MODIFY COLUMN `s_id` varchar(255) NULL DEFAULT NULL,MODIFY COLUMN `c_id` varchar(255) NULL DEFAULT NULL".replace("#", type));
                baseSql.execute("ALTER TABLE `commission_#` MODIFY COLUMN `s_id` varchar(255) NULL DEFAULT NULL,MODIFY COLUMN `c_id` varchar(255) NULL DEFAULT NULL".replace("#", type));

            } catch (Exception e) {
                log.error(type);
            }
        }
    }

    @Test
    public void clean() throws SQLException {
        List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type");
        for (GroovyRowResult row : rows) {
            String type = row.get("type").toString();
            tabSideData.setDefaultSideFlag(type);
        }
    }

    String[] tables = new String[]{"das_data_pool", "das_data_pool_business", "das_data_pool_business_1", "das_data_pool_business_14", "das_data_pool_business_16", "das_data_pool_business_18", "das_data_pool_business_2002", "das_data_pool_business_2005_110000", "das_data_pool_business_2005_120000", "das_data_pool_business_2005_130000", "das_data_pool_business_2005_140000", "das_data_pool_business_2005_150000", "das_data_pool_business_2005_210000", "das_data_pool_business_2005_220000", "das_data_pool_business_2005_230000", "das_data_pool_business_2005_310000", "das_data_pool_business_2005_320000", "das_data_pool_business_2005_330000", "das_data_pool_business_2005_340000", "das_data_pool_business_2005_350000", "das_data_pool_business_2005_360000", "das_data_pool_business_2005_370000", "das_data_pool_business_2005_410000", "das_data_pool_business_2005_420000", "das_data_pool_business_2005_430000", "das_data_pool_business_2005_440000", "das_data_pool_business_2005_450000", "das_data_pool_business_2005_460000", "das_data_pool_business_2005_500000", "das_data_pool_business_2005_510000", "das_data_pool_business_2005_520000", "das_data_pool_business_2005_530000", "das_data_pool_business_2005_540000", "das_data_pool_business_2005_610000", "das_data_pool_business_2005_620000", "das_data_pool_business_2005_630000", "das_data_pool_business_2005_640000", "das_data_pool_business_2005_650000", "das_data_pool_business_2007", "das_data_pool_business_2011", "das_data_pool_business_2016", "das_data_pool_business_2019", "das_data_pool_business_2021", "das_data_pool_business_2022", "das_data_pool_business_2023", "das_data_pool_business_2024", "das_data_pool_business_2026", "das_data_pool_business_2027", "das_data_pool_business_2041", "das_data_pool_business_2042", "das_data_pool_business_2043", "das_data_pool_business_2044", "das_data_pool_business_2045", "das_data_pool_business_2046", "das_data_pool_business_2050", "das_data_pool_business_2056", "das_data_pool_business_2058", "das_data_pool_business_2060", "das_data_pool_business_2062", "das_data_pool_business_2065", "das_data_pool_business_2066", "das_data_pool_business_2072", "das_data_pool_business_2073", "das_data_pool_business_2076", "das_data_pool_business_2085", "das_data_pool_business_2088", "das_data_pool_business_2090", "das_data_pool_business_2095", "das_data_pool_business_2096", "das_data_pool_business_21", "das_data_pool_business_2100", "das_data_pool_business_2101", "das_data_pool_business_2109", "das_data_pool_business_25", "das_data_pool_business_26", "das_data_pool_business_27", "das_data_pool_business_32", "das_data_pool_business_35", "das_data_pool_business_36", "das_data_pool_business_4", "das_data_pool_business_40", "das_data_pool_business_4002", "das_data_pool_business_45", "das_data_pool_business_46", "das_data_pool_business_49", "das_data_pool_business_50", "das_data_pool_business_52", "das_data_pool_business_54", "das_data_pool_business_55", "das_data_pool_business_57", "das_data_pool_business_62"};

    @Test
    public void initBusiness() throws InterruptedException {

        ThreadPoolUtils.executeRun(Lists.newArrayList(tables), table -> {
            try {
                baseSql.execute("update # set handle_sign=0".replace("#", table));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }).await();
    }

    @Test
    public void delete() throws SQLException {
        List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type");
        for (GroovyRowResult row : rows) {
            String type = row.get("type").toString();
            baseSql.executeUpdate("delete a.* from result_gross_margin_ref a,result_#_2 b where a.table_name='result_#_2' and a.result_id=b.id and b.handle_sign=7".replace("#", type));
        }
    }
}