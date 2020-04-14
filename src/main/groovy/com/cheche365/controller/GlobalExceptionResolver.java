package com.cheche365.controller;


import com.cheche365.entity.RestResponse;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import javax.servlet.http.HttpServletResponse;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 全局Controller层异常处理类
 */

@ControllerAdvice
@Log4j2
public class GlobalExceptionResolver {

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    public RestResponse handleException(Exception e, HttpServletResponse response) {
        log.error("服务器错误", e);
        Map<Object, String> exceptionStatckTrace = new LinkedHashMap<>();
        exceptionStatckTrace.put(0, e.getClass().getName() + ": " + e.getMessage());
        StackTraceElement[] stackTrace = e.getStackTrace();
        int length = stackTrace.length;
        StackTraceElement obj = null;
        StringBuilder fileName = null;
        for (int i = 0; i < length; i++) {
            obj = stackTrace[i];
            fileName = new StringBuilder();
            if (obj.getFileName() != null) {
                fileName.append(obj.getFileName());
                if (obj.getLineNumber() != -1) {
                    fileName.append(": ").append(obj.getLineNumber());
                }
            } else {
                fileName.append("Unknown Source");
            }
            exceptionStatckTrace.put(i + 1, obj.getClassName() + "." + obj.getMethodName() + "(" + fileName.toString() + ")");
        }

        return RestResponse.failed(e.getMessage(), exceptionStatckTrace);
    }
}
