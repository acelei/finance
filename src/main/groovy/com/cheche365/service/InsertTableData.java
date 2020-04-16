package com.cheche365.service;

import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;

/**
 * author:WangZhaoliang
 * Date:2020/4/15 10:49
 */
@Service
@Log4j2
public class InsertTableData {

    @Autowired
    private Sql baseSql;

    public void insertData() throws SQLException {
        List<GroovyRowResult> groovyRowResultList = baseSql.rows("select * from table_type where id >= 60 and id <= 67");
        for (GroovyRowResult groovyRowResult : groovyRowResultList) {
            String type = groovyRowResult.get("type").toString();
            String sourceFile = groovyRowResult.get("source_file").toString();
            baseSql.execute("truncate table commission_" + type);
            baseSql.execute("truncate table settlement_" + type);

//            baseSql.execute("create table commission_" + type +" like commission_bj");
//            baseSql.execute("create table settlement_" + type + " like settlement_bj");
//            baseSql.execute("create table result_" + type + " like result_bj");
//            baseSql.execute("create table result_" + type + "_2 like result_bj_2");
//            baseSql.execute("create table result_" + type + "_3 like result_bj_3");
//            baseSql.execute("create table result_" + type + "_2_final like result_bj_2_final");
//            baseSql.execute("create table result_" + type + "_back like result_bj_back");
//            baseSql.execute("create table result_" + type + "_final like result_bj_final");

            baseSql.executeInsert("insert into commission_" + type + " select * from settlement_commission_temp where source_file= '" + sourceFile + "'");
            baseSql.executeInsert("insert into settlement_" + type + " select * from commission_" + type);
            log.info("handle success! type:{}, sourceFile:{}", type, sourceFile);
        }
    }

}
