package com.cheche365.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Log4j2
public class ExcelUtil2Test {
    @Test
    public void WriteToExcel() throws InterruptedException, ExecutionException, IOException {
        log.info("开始");
        BlockingDeque<Map> dataList = Queues.newLinkedBlockingDeque(10000);

        Future<File> f = ThreadPoolUtils.getRunPool().submit(() -> ExcelUtil2.writeToExcelByQueue(dataList));


        for (int i = 0; i < ExcelUtil2.EXCEL_SHEET_MAX_ROWS - 2; i++) {
            Map<String, Object> map = new HashMap<>();
            for (int j = 0; j < 20; j++) {
                map.put("字段" + j, "" + i + RandomStringUtils.randomAlphanumeric(20));
            }

            dataList.put(map);
        }
        dataList.put(ExcelUtil2.EMPTY_MAP);

        File file = f.get();

        log.info("文件已经输出完成");

        FileUtils.copyFile(file, new File("123.xlsx"));
        file.delete();

        log.info("结束");
    }

    @Test
    public void WriteToExcel2() throws IOException {
        log.info("开始");
        List<Map> dataList = new ArrayList<>();

        for (int i = 0; i < 200000; i++) {
            Map<String, Object> map = new HashMap<>();
            for (int j = 0; j < 20; j++) {
                map.put("字段" + j, "" + i + RandomStringUtils.randomAlphanumeric(20));
            }

            dataList.add(map);
        }

        File file = ExcelUtil2.writeToExcel(dataList);

        FileUtils.copyFile(file, new File("123.xlsx"));
        file.delete();
        log.info("结束");
    }

    @Test
    public void readExcel() throws IOException {
        ExcelUtil2.getSheet(null, "0");
    }

}
