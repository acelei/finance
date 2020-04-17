package com.cheche365.service

import com.cheche365.util.ThreadPoolUtils
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.corpus.tag.Nature
import com.hankcs.hanlp.seg.Segment
import com.hankcs.hanlp.seg.common.Term
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

import javax.annotation.PostConstruct
import java.util.concurrent.CopyOnWriteArrayList

@Service
@Slf4j
class FixInsuranceCompanyArea {
    public Segment segment
    private Map insuranceCompany = new HashMap()
    private List tables = ["commission", "settlement"]
    private Map area = new HashMap()

    @Autowired
    Sql baseSql

    @PostConstruct
    void init() {
        area = [延边朝: "吉林省", 恩施: "湖北省", 湘西: "湖南省", 阿坝: "四川省", 甘孜: "四川省", 凉山: "四川省", 黔西南: "贵州省", 黔东南: "贵州省", 黔南: "贵州省", 楚雄: "云南省", 红河: "云南省", 文山: "云南省", 西双版纳: "云南省", 大理: "云南省", 德宏: "云南省", 怒江: "云南省", 迪庆: "云南省", 临夏: "甘肃省", 甘南: "甘肃省", 海北: "青海省", 黄南: "青海省", 海南: "青海省", 果洛: "青海省", 玉树: "青海省", 海西: "青海省", 昌吉: "新疆维吾尔自治区", 博尔塔拉: "新疆维吾尔自治区", 巴音郭楞: "新疆维吾尔自治区", 克孜勒: "新疆维吾尔自治区", 伊犁: "新疆维吾尔自治区", 内蒙古: "内蒙古自治区", 广西: "广西壮族自治区", 西藏: "西藏自治区", 宁夏: "宁夏回族自治区", 新疆: "新疆维吾尔自治区"]
        HanLP.Config.CustomDictionaryPath = ["custom/insuranceCompany.txt ntc", "custom/全国地名大全.txt ns", "custom/机构名词典.txt nt"]
        segment = HanLP.newSegment().enableCustomDictionaryForcing(true).enableOrganizationRecognize(true).enableNameRecognize(false)
        baseSql.eachRow("select id,name from insurance_company where level=1") {
            insuranceCompany.put(it.name, it.id)
        }
        baseSql.eachRow("select a.name as a,b.name as b from area a,area b where concat(substr(a.id,1,2),'0000')=b.id and a.type in (1,2,3) and b.type in (1,2)") {
            area.put(it.a, it.b)
            area.put((it.a).replaceAll("市|地区|省", ""), it.b)
        }
    }

    void run(String type) {
        ThreadPoolUtils.executeTask(tables, { it ->
            log.info("处理开始:${it}_${type}")
            log.info("拆分保险公司名称开始:${it}_${type}")
            def list = tableRun("select `7-出单保险公司（明细至保险公司分支机构）`,`保险公司`,`市`,`省`,`保险公司id` from ${it}_${type} where `7-出单保险公司（明细至保险公司分支机构）` is not null and (`保险公司` is null or `省` is null) group by `7-出单保险公司（明细至保险公司分支机构）` ")
            log.info("拆分保险公司名称完成:${it}_${type}")
            log.info("更新保险公司名称及ID开始:${it}_${type}")
            updateRun(list, "${it}_${type}")
            log.info("更新保险公司名称及ID完成:${it}_${type}")
            log.info("处理完毕:${it}_${type}")
        }).await()
    }

    void runTable(String tableName) {
        log.info("处理开始:${tableName}")
        log.info("拆分保险公司名称开始:${tableName}")
        def list = tableRun("select `7-出单保险公司（明细至保险公司分支机构）`,`保险公司`,`市`,`省`,`保险公司id` from ${tableName} where `7-出单保险公司（明细至保险公司分支机构）` is not null and (`保险公司` is null or `省` is null) group by `7-出单保险公司（明细至保险公司分支机构）` ")
        log.info("拆分保险公司名称完成:${tableName}")
        log.info("更新保险公司名称及ID开始:${tableName}")
        updateRun(list, tableName)
        log.info("更新保险公司名称及ID完成:${tableName}")
        log.info("处理完毕:${tableName}")
    }

    List<GroovyRowResult> tableRun(String sql) {
        List<GroovyRowResult> list = new CopyOnWriteArrayList<>()
        ThreadPoolUtils.executeRun(baseSql.rows(sql), {
            list.add(setInsuranceCompanyArea(it))
        }).await()

        return list
    }

    void updateRun(List<GroovyRowResult> list, String table) {
        list.each {
            update(table, it)
        }
    }

    GroovyRowResult setInsuranceCompanyArea(GroovyRowResult row) {
        List<Term> tList = segment.seg((row.'7-出单保险公司（明细至保险公司分支机构）').replaceAll("分公司|支公司|中心", ""))
        log.debug("保险公司分词:{}", tList)
        Term t = tList.find { it.nature == Nature.ntc }
        log.debug("确认保险公司:{}", t)
        if (t) {
            row.'保险公司' = t.word
        } else {
            for (int i = 0; i < tList.size(); i++) {
                Term term = tList.get(i)
                if (term.nature == Nature.nt) {
                    if (term.word.startsWith("财产") || term.word.startsWith("保险")) {
                        row.'保险公司' = tList.get(i - 1).word + term.word
                    } else {
                        row.'保险公司' = term.word
                    }
                    break
                }
            }
        }

        for (Term term : tList) {
            if (term.nature == Nature.ns && term.word != "中国" && term.word != "太平洋") {
                row.'市' = term.word
                break
            }
        }

        if (row.'保险公司' != null || row.'市' != null) {
            if (row.'保险公司' != null) {
                row.'保险公司id' = insuranceCompany.get(row.'保险公司')
            }
            if (row.'市' != null) {
                row.'省' = area.get(row.'市')
            }
            return row
        } else {
            return null
        }
    }

    void update(String table, GroovyRowResult row) {
        if (row != null) {
            String sql = "update ${table} set `保险公司`=?,`保险公司id`=?,`省`=?,`市`=? where `保险公司` is null and `7-出单保险公司（明细至保险公司分支机构）`=?"
            baseSql.executeUpdate(sql, row.'保险公司', row.'保险公司id', row.'省', row.'市', row.'7-出单保险公司（明细至保险公司分支机构）')
        }
    }
}
