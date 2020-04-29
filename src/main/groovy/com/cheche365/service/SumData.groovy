package com.cheche365.service

import com.cheche365.util.ExcelUtil2
import com.cheche365.util.ThreadPool
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class SumData {
    @Autowired
    private Sql baseSql
    @Autowired
    private ThreadPool runThreadPool

    static final String sql = '''
insert into result_sum_data_3 (`id`,
                               `s_id`,
                               `d_id`,
                               `c_id`,
                               `source_file`,
                               `1-序号`,
                               `2-保代机构`,
                               `3-出单保险代理机构（车车科技适用）`,
                               `4-发票付款方（与发票一致）`,
                               `5-投保人名称`,
                               `6-保单单号`,
                               `7-出单保险公司（明细至保险公司分支机构）`,
                               `8-险种名称`,
                               `9-保单出单日期`,
                               `10-全保费`,
                               `11-净保费`,
                               `12-手续费等级（对应点位台账）`,
                               `13-手续费率`,
                               `14-手续费总额（报行内+报行外）(含税)`,
                               `15-手续费总额（报行内+报行外）(不含税)`,
                               `16-收入入账月度`,
                               `17-凭证号`,
                               `18-手续费比例`,
                               `19-手续费金额（含税）`,
                               `20-手续费金额（不含税）`,
                               `21-回款月度`,
                               `22-凭证号`,
                               `23-收款金额`,
                               `24-开票单位`,
                               `25-开票日期`,
                               `26-手续费比例`,
                               `27-开票金额（不含税）`,
                               `28-开票金额（含税）`,
                               `29-20191231应收账款（含已开票和未开票）`,
                               `30-开票日期`,
                               `31-应收回款月度`,
                               `32-收款凭证号`,
                               `33-收款金额`,
                               `34-开票单位`,
                               `35-开票日期`,
                               `36-开票金额（不含税）`,
                               `37-开票金额（含税）`,
                               `38-尚未开票金额（不含税）`,
                               `39-尚未开票金额（含税）`,
                               `40-代理人名称`,
                               `41-佣金比例`,
                               `42-佣金金额（已入账）`,
                               `43-支付主体`,
                               `44-支付比例`,
                               `45-支付金额`,
                               `46-未计提佣金（19年底尚未入帐）`,
                               `保险公司`,
                               `省`,
                               `市`,
                               `保险公司id`,
                               `handle_sign`,
                               `sum_fee`,
                               `sum_commission`,
                               `gross_profit`,
                               `type_id`)
select `id`,
       `s_id`,
       `d_id`,
       `c_id`,
       `source_file`,
       `1-序号`,
       `2-保代机构`,
       `3-出单保险代理机构（车车科技适用）`,
       `4-发票付款方（与发票一致）`,
       `5-投保人名称`,
       `6-保单单号`,
       `7-出单保险公司（明细至保险公司分支机构）`,
       `8-险种名称`,
       `9-保单出单日期`,
       `10-全保费`,
       `11-净保费`,
       `12-手续费等级（对应点位台账）`,
       `13-手续费率`,
       `14-手续费总额（报行内+报行外）(含税)`,
       `15-手续费总额（报行内+报行外）(不含税)`,
       `16-收入入账月度`,
       `17-凭证号`,
       `18-手续费比例`,
       `19-手续费金额（含税）`,
       `20-手续费金额（不含税）`,
       `21-回款月度`,
       `22-凭证号`,
       `23-收款金额`,
       `24-开票单位`,
       `25-开票日期`,
       `26-手续费比例`,
       `27-开票金额（不含税）`,
       `28-开票金额（含税）`,
       `29-20191231应收账款（含已开票和未开票）`,
       `30-开票日期`,
       `31-应收回款月度`,
       `32-收款凭证号`,
       `33-收款金额`,
       `34-开票单位`,
       `35-开票日期`,
       `36-开票金额（不含税）`,
       `37-开票金额（含税）`,
       `38-尚未开票金额（不含税）`,
       `39-尚未开票金额（含税）`,
       `40-代理人名称`,
       `41-佣金比例`,
       `42-佣金金额（已入账）`,
       `43-支付主体`,
       `44-支付比例`,
       `45-支付金额`,
       `46-未计提佣金（19年底尚未入帐）`,
       `保险公司`,
       `省`,
       `市`,
       `保险公司id`,
       `handle_sign`,
       `sum_fee`,
       `sum_commission`,
       `gross_profit`,
       ?
from result_#_3
'''

    String errTjSql = "select ':type' as '业务名称','整合表' as '数据表','总计' as '描述',count(9) as '条目数',sum(sum_fee) as '收入',sum(sum_commission) as '成本' from result_#_back\n" +
            "union all\n" +
            "select ':type','整合表','未调整数据',count(9),sum(sum_fee),sum(sum_commission) from result_#_2 where handle_sign in (0,6)\n" +
            "union all\n" +
            "select ':type','整合表','未替换数据',count(9),sum(sum_fee),sum(sum_commission) from result_#_2 where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and handle_sign=3\n" +
            "union all\n" +
            "select ':type','整合表','替换数据',count(9),sum(sum_fee),sum(sum_commission) from result_#_2 where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and handle_sign=9\n" +
            "union all\n" +
            "select ':type','整合表','硬调整数据',count(9),sum(sum_fee),sum(sum_commission) from result_#_2 where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and handle_sign=10\n" +
            "union all\n" +
            "select ':type','收入表','剩余收入数据',count(9),sum(sum_fee),sum(sum_commission) from settlement_# where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and handle_sign=6\n" +
            "union all\n" +
            "select ':type','收入表','剩余收入负数',count(9),sum(sum_fee),sum(sum_commission) from settlement_# where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and handle_sign=6 and sum_fee<0\n" +
            "union all\n" +
            "select ':type','成本表','剩余成本数据',count(9),sum(sum_fee),sum(sum_commission) from commission_# where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and handle_sign=6\n" +
            "union all\n" +
            "select ':type','成本表','剩余成本负数',count(9),sum(sum_fee),sum(sum_commission) from commission_# where `8-险种名称` in ('交强险','商业险') and `9-保单出单日期`>'2019' and handle_sign=6 and sum_commission<0"

    void sumResult3(String type, Integer id) {
        baseSql.executeInsert(sql.replace("#", type), [id])
    }

    List<String> head = ["业务名称", "数据表", "描述", "条目数", "收入", "成本"]

    File statisticsAll(String typeSql) {
        def rows = baseSql.rows(typeSql)
        List<GroovyRowResult> resultList = runThreadPool.submitWithResult(rows, { row ->
            def type = row.type
            def name = row.name
            baseSql.rows(errTjSql.replace("#", type).replace(":type", name))
        }).flatten()

        return ExcelUtil2.writeToExcel(head, resultList)
    }

    File statistics(String type, String name) {
        def rows = baseSql.rows(errTjSql.replace("#", type).replace(":type", name))
        return ExcelUtil2.writeToExcel(head, rows)
    }

}
