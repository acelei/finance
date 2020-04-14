package com.cheche365.util;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.monitorjbl.xlsx.StreamingReader;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.OLE2NotOfficeXmlFileException;
import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.apache.poi.ss.SpreadsheetVersion;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFCell;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.springframework.util.Assert;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

/**
 * Excel util 2
 *
 * @author wanglei
 * @date 2020年2月12日 下午2:21:52
 */
@Log4j2
public class ExcelUtil2 {
    public static Map EMPTY_MAP = Maps.newHashMapWithExpectedSize(0);

    /**
     * excel的最大行
     */
    public static final int EXCEL_SHEET_MAX_ROWS = SpreadsheetVersion.EXCEL2007.getMaxRows() - 1;
    public static final Pattern NUMBER_PATTERN = Pattern.compile("^[-\\+]?[\\d]*$");

    /**
     * 根据时间创建Excel文件名称
     *
     * @return the 文件名称
     */
    public static String generateExportExcelName() {
        String dateString = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        return "export_" + dateString + ".xlsx";
    }

    /**
     * 获取单元格值
     * <p>
     * 如果表格中有公式的情况则使用公式获取
     *
     * @param cell             单元格对象
     * @param formulaEvaluator 表格格式计算器
     * @return 单元格值
     */
    private static Object getCellValue(Cell cell, FormulaEvaluator formulaEvaluator) {
        Object value = null;
        if (cell != null) {
            switch (cell.getCellType()) {
                case STRING:
                    value = cell.getStringCellValue().trim();
                    break;
                case BOOLEAN:
                    value = cell.getBooleanCellValue();
                    break;
                case NUMERIC:
                    if (DateUtil.isCellDateFormatted(cell)) {
                        value = cell.getDateCellValue();
//                        Date dateTemp = cell.getDateCellValue();
//                        //将java.util.Date转换成java.time.LocalDate
//                        LocalDateTime localDateTime = LocalDateTime.ofInstant(dateTemp.toInstant(), ZoneId.systemDefault());
//                        value = localDateTime.toLocalDate();
                    } else {
                        value = new BigDecimal(BigDecimal.valueOf(cell.getNumericCellValue()).toPlainString());
                    }
                    break;
                case FORMULA:
                    if (formulaEvaluator == null) {
                        value = cell.getCellFormula();
                    } else {
                        value = getCellValue(formulaEvaluator.evaluate(cell));
                    }

                    break;
                default:
                    break;
            }
        }

        return value;
    }

    /**
     * 使用公式获取单元格值
     *
     * @param cell 格式计算器输出的单元格值
     * @return 单元格值
     */
    private static Object getCellValue(CellValue cell) {
        Object value = null;
        if (cell != null) {
            switch (cell.getCellType()) {
                case STRING:
                    value = cell.getStringValue().trim();
                    break;
                case BOOLEAN:
                    value = cell.getBooleanValue();
                    break;
                case NUMERIC:
                    value = cell.getNumberValue();
                    break;
                default:
                    break;
            }
        }
        return value;
    }

    /**
     * 设置单元格值
     * <p>
     * 统一使用文本形式输出,可控制表格格式减小体积
     *
     * @param cell  单元格对象
     * @param value 单元格值
     */
    private static void setCellValue(Cell cell, Object value) {
        if (value == null) {
            cell.setCellValue("");
        } else if (value instanceof Number) {
            cell.setCellValue(Double.parseDouble(value.toString()));
        } else if (value instanceof LocalDate) {
            cell.setCellValue(((LocalDate) value).format(DateTimeFormatter.ISO_LOCAL_DATE));
        } else if (value instanceof Date) {
            cell.setCellValue(new SimpleDateFormat("yyyy-MM-dd").format((Date) value));
        } else {
            String string = value.toString();
            cell.setCellValue(string.length() > 1000 ? string.substring(0, 1000) : string);
        }
    }

    /**
     * 将map转换为Excel文件
     * <p>
     * 文件输出为临时文件,根据头映射信息进行转换
     * <p>
     * 例如:
     * headMap:[orderMonth:月份, fileName:文件名称, sheetName:sheet, rowCount:行数, orderFee:收入, orderCommission:成本]
     * data:[
     * [fileName:201906-02-车险财务模板-优信1.5-科技公对公支付.xlsx, orderMonth:2019-06, sheetName:Sheet1, orderFee:0.00, rowCount:418, orderCommission:101182.98],
     * [fileName:201906-02-车险财务模板-优信4.5-科技公对公支付.xlsx, orderMonth:2019-06, sheetName:Sheet1, orderFee:232242.14, rowCount:1187, orderCommission:223053.65]
     * ]
     * result:
     * <p>
     * 月份       文件名称	                                    sheet	行数	收入	    成本
     * 2019-06  201906-02-车险财务模板-优信1.5-科技公对公支付.xlsx	Sheet1	418	0	101182.98
     * 2019-06	201906-02-车险财务模板-优信4.5-科技公对公支付.xlsx	Sheet1	1187	232242.14	223053.65
     *
     * @param headMap   表格头Key-value映射
     * @param dataQueue 数据列表
     * @return 输出Excel文件
     * @throws IOException exception
     */
    public static File writeToExcel(Map<String, String> headMap, BlockingQueue<Map> dataQueue, File file) throws IOException {
        Assert.isTrue(MapUtils.isNotEmpty(headMap), "表头信息不能为空");
        Assert.notNull(dataQueue, "数据不能为空");

        SXSSFWorkbook workbook = new SXSSFWorkbook(1000);
        SXSSFSheet sheet = workbook.createSheet();

        if (file == null) {
            file = File.createTempFile(generateExportExcelName(), ".xlsx");
        }
        FileUtils.forceMkdirParent(file);

        try (FileOutputStream fos = new FileOutputStream(file)) {
            // 写入表头
            int rowNum = 0;

            Map<String, Integer> mapKey = Maps.newHashMapWithExpectedSize(headMap.size());
            SXSSFRow head = sheet.createRow(rowNum++);
            int columnIndex = 0;
            for (Map.Entry<String, String> entry : headMap.entrySet()) {
                int index = columnIndex++;
                String key = entry.getKey();
                String value = entry.getValue();
                SXSSFCell cell = head.createCell(index);
                setCellValue(cell, value);
                mapKey.put(key, index);
            }

            try {
                while (true) {
                    Map data = dataQueue.take();

                    if (data == EMPTY_MAP || Thread.currentThread().isInterrupted()) {
                        break;
                    }

                    SXSSFRow row = sheet.createRow(rowNum++);
                    for (Map.Entry<String, Integer> entry : mapKey.entrySet()) {
                        SXSSFCell cell = row.createCell(entry.getValue());
                        setCellValue(cell, data.get(entry.getKey()));
                    }

                    // 如果数据行数超过Excel限制则在最后一行写入提示信息
                    if (rowNum >= EXCEL_SHEET_MAX_ROWS) {
                        log.warn("超出Excel文件限制无法继续导出");
                        SXSSFRow lastRow = sheet.createRow(rowNum);
                        lastRow.createCell(0).setCellValue("超出Excel文件限制无法继续导出");
                        break;
                    }

                    // 每大于1000行时则将数据刷入磁盘xml
                    if (rowNum % 5000 == 0) {
                        sheet.flushRows();
                    }
                }
            } catch (InterruptedException e) {
                log.warn("线程中断", e);
            }

            workbook.write(fos);
        } finally {
            // 处理SXSSFWorkbook导出excel时，产生的临时文件
            workbook.dispose();
        }

        return file;
    }

    /**
     * 将List转换成queue进行数据导入
     * <p>
     * 使用队列的进行数据导入 {@link #writeToExcel(Map, BlockingQueue, File)}
     *
     * @param headList head list
     * @param dataMap  data map
     * @return the file
     * @throws IOException io exception
     */
    private static File writeToExcel(Map<String, String> headList, List<Map> dataMap) throws IOException {
        LinkedBlockingQueue<Map> dataQueue = Queues.newLinkedBlockingQueue(dataMap);

        dataQueue.add(EMPTY_MAP);
        return writeToExcel(headList, dataQueue, null);
    }

    /**
     * 将map转换为Excel文件
     * <p>
     * 直接转换表头信息为map key信息,顺序不定
     * <p>
     * 使用基础转换功能完成 {@link #writeToExcel(Map, List)}
     *
     * @param dataMap 数据列表
     * @return 输出Excel文件
     * @throws IOException exception
     */
    public static File writeToExcel(List<Map> dataMap) throws IOException {
        if (CollectionUtils.isEmpty(dataMap)) {
            log.warn("数据内容为空");
            return null;
        }

        Map<String, Object> firstRow = dataMap.get(0);
        HashMap<String, String> headList = initHeadMap(firstRow);


        return writeToExcel(headList, dataMap);
    }

    /**
     * 将map转换为Excel文件
     * <p>
     * 转换map中指定key属性
     * <p>
     * 使用基础转换功能完成 {@link #writeToExcel(Map, List)}
     *
     * @param head    表格头信息
     * @param dataMap 数据列表
     * @return 输出Excel文件
     * @throws IOException exception
     */
    public static File writeToExcel(List<String> head, List<Map> dataMap) throws IOException {
        Assert.isTrue(CollectionUtils.isNotEmpty(head), "表头信息不能为空");
        LinkedHashMap<String, String> headList = initHeadMap(head);

        return writeToExcel(headList, dataMap);
    }

    public static File writeToExcel(List<String> head, BlockingDeque<Map> dataQueue) throws IOException {
        Assert.isTrue(CollectionUtils.isNotEmpty(head), "表头信息不能为空");
        LinkedHashMap<String, String> headList = initHeadMap(head);

        return writeToExcel(headList, dataQueue, null);
    }

    /**
     * 使用阻塞队列进行数据导出
     * <p>
     * 注意如果使用队列直接导入时,在初始化表头时需要一行数据,所以在初始化表头后,需要将此行数据放入队列首位
     * 所以此处需要双向队列来保证数据顺序
     * 注意:建议数据导出队列大小设置在10000
     * 数据读取完成后需要在队列最后加入EMPTY_MAP对象用于控制线程结束 {@link #EMPTY_MAP} 不然线程无法结束
     *
     * @param dataQueue 数据队列
     * @return 输出Excel文件
     * @throws IOException          io exception
     * @throws InterruptedException interrupted exception
     */
    public static File writeToExcelByQueue(BlockingDeque<Map> dataQueue) throws IOException, InterruptedException {
        Map<String, Object> firstRow = dataQueue.take();
        dataQueue.addFirst(firstRow);

        HashMap<String, String> headList = initHeadMap(firstRow);

        return writeToExcel(headList, dataQueue, null);
    }

    /**
     * 根据数map初始化表头映射
     *
     * @param firstRow 一行数据map
     * @return the hash map
     */
    private static HashMap<String, String> initHeadMap(Map<String, Object> firstRow) {
        HashMap<String, String> headList = Maps.newHashMapWithExpectedSize(firstRow.size());
        for (String key : firstRow.keySet()) {
            headList.put(key, key);
        }
        return headList;
    }

    /**
     * 根据list初始化表头映射
     *
     * @param head 表头list配置
     * @return the linked hash map
     */
    private static LinkedHashMap<String, String> initHeadMap(List<String> head) {
        LinkedHashMap<String, String> headList = Maps.newLinkedHashMapWithExpectedSize(head.size());
        for (String key : head) {
            headList.put(key, key);
        }
        return headList;
    }

    /**
     * 获取sheet对象
     * <p>
     * 使用流式读取时占用内存较小但是不能是用公式
     * 会先使用XSSF形式流式读取,如果失败则使用HSSF进行读取
     *
     * @param inputStream 输入流
     * @param sheetName   sheet名称 可以是数字字符串,如为数字字符串将已index方式获取sheet
     * @return the sheet
     * @throws IOException io exception
     */
    public static Sheet getSheet(InputStream inputStream, String sheetName) throws IOException {
        Assert.notNull(inputStream, "输入流不能为空");
        Workbook workbook;
        ZipSecureFile.setMinInflateRatio(0D);
        try {
            workbook = StreamingReader.builder()
                    .rowCacheSize(1000)
                    .bufferSize(4096)
                    .open(inputStream);
        } catch (OLE2NotOfficeXmlFileException e) {
            log.warn("Excel为97格式将使用HSSF形式读取.可以使用公式,但是占用内存较高.");
            workbook = new HSSFWorkbook(inputStream);
        }

        Sheet sheet = null;
        if (isInteger(sheetName)) {
            int sheetIndex = Integer.parseInt(sheetName);
            if (sheetIndex < 0 || workbook.getNumberOfSheets() < sheetIndex + 1) {
                sheetIndex = 0;
            }
            sheet = workbook.getSheetAt(sheetIndex);
        } else {
            sheet = workbook.getSheet(sheetName);
        }

        return sheet;
    }


    /**
     * 判断是否是整数
     *
     * @param str str
     * @return the boolean
     */
    private static boolean isInteger(String str) {
        if (StringUtils.isEmpty(str)) {
            return false;
        }
        return NUMBER_PATTERN.matcher(str).matches();
    }
}
