package com.ververica.cdc.connectors.mysql.source.sf;

public class ResponseData<T> {

    private int status;

    private String errMsg;

    private T data;

    public ResponseData() {}

    public ResponseData(int status, String errMsg, T data) {
        this.status = status;
        this.errMsg = errMsg;
        this.data = data;
    }

    public ResponseData(int status, T data) {
        this.status = status;
        this.data = data;
    }

    public static <T> ResponseData<T> out(int code, T data) {
        return new ResponseData<T>(code, data);
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }
}
