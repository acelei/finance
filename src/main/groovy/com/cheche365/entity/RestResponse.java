package com.cheche365.entity;

public class RestResponse<T> {
    private Integer restCode;
    private String restMsg;
    private T restContext;


    public RestResponse(Integer retCode) {
        if (retCode >= 1) {
            this.restCode = 1;
            this.restMsg = "success";
        } else {
            this.restCode = -1;
            this.restMsg = "failed";
        }
    }

    public RestResponse(Integer retCode, String retMsg) {
        this.restCode = retCode;
        this.restMsg = retMsg;
    }

    public RestResponse(Integer retCode, String retMsg, T retContext) {
        this.restCode = retCode;
        this.restMsg = retMsg;
        this.restContext = retContext;
    }

    public Integer getRestCode() {
        return restCode;
    }

    public void setRestCode(Integer restCode) {
        this.restCode = restCode;
    }

    public String getRestMsg() {
        return restMsg;
    }

    public void setRestMsg(String restMsg) {
        this.restMsg = restMsg;
    }

    public T getRestContext() {
        return restContext;
    }

    public void setRestContext(T restContext) {
        this.restContext = restContext;
    }

    public Boolean isSuccess() {
        return this.restCode == 1;
    }

    public static RestResponse<String> failed() {
        return failed("failed", null);
    }

    public static RestResponse<String> failedMessage(String retMsg) {
        return failed(retMsg, null);
    }

    public static <T> RestResponse<T> failed(T retContext) {
        return failed("failed", retContext);
    }

    public static <T> RestResponse<T> failed(String retMsg, T retContext) {
        return new RestResponse<>(-1, retMsg, retContext);
    }

    public static RestResponse<String> success() {
        return success("success", null);
    }

    public static RestResponse<String> successMessage(String retMsg) {
        return success(retMsg, null);
    }

    public static <T> RestResponse<T> success(T retContext) {
        return success("success", retContext);
    }

    public static <T> RestResponse<T> success(String retMsg, T retContext) {
        return new RestResponse<>(1, retMsg, retContext);
    }

    /**
     * 处理失败，并且需要前端弹弹窗
     * @param restMsg ： 弹窗标题
     * @param retContext ： 弹窗内容
     * @param <T>
     * @return
     */
    public static <T> RestResponse<T> failedToast(String restMsg, T retContext) {
        return new RestResponse<>(460, restMsg, retContext);
    }

    public boolean isFailedToast() {
        return this.restCode == 460;
    }
}
