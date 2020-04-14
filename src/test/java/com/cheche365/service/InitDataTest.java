package com.cheche365.service;

import app.SpringApplicationLauncher;
import com.cheche365.util.ThreadPoolUtils;
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
        String type = "yx";
        initData.roll(type);
        initData.fixPremium(type, "");
    }


    String[] types = new String[]{"chongqing", "guangdong", "chengshuo", "henan", "liaoning", "guangxi", "jiangsu"};

    @Test
    public void createTable() throws SQLException {
        String type = "tj";

        baseSql.execute("create table settlement_# like settlement_bj".replace("#",type));
        baseSql.execute("create table commission_# like commission_bj".replace("#",type));
        baseSql.execute("create table result_# like result_bj".replace("#",type));
        baseSql.execute("create table result_#_2 like result_bj_2".replace("#",type));
        baseSql.execute("create table result_#_back like result_bj_back".replace("#",type));
        baseSql.execute("create table result_#_3 like result_bj_3".replace("#",type));
        baseSql.execute("create table result_#_final like result_bj_final".replace("#",type));
        baseSql.execute("create table result_#_2_final like result_bj_2_final".replace("#",type));

    }

    @Test
    public void modifyTable() throws SQLException {
        List<GroovyRowResult> rows = baseSql.rows("select `type` from table_type");

        for (GroovyRowResult row : rows) {
            String type = row.get("type").toString();
            try {
                baseSql.execute("ALTER TABLE `result_#_final`  ADD COLUMN `sum_fee` varchar(100) NULL,ADD COLUMN `sum_commission` varchar(100) NULL,ADD COLUMN `gross_profit` varchar(100) NULL".replace("#", type));
                baseSql.execute("ALTER TABLE `result_#_2_final`  ADD COLUMN `sum_fee` varchar(100) NULL,ADD COLUMN `sum_commission` varchar(100) NULL,ADD COLUMN `gross_profit` varchar(100) NULL".replace("#", type));

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
}