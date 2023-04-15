package com.ververica.cdc.connectors.sf.entity;

import java.io.Serializable;

/** encrypt field. @Author: created by eHui @Date: 2022/5/27 */
public class EncryptField implements Serializable {

    private String fieldName;
    private EncryptType encryptType;

    public EncryptField(String fieldName, EncryptType encryptType) {
        this.fieldName = fieldName;
        this.encryptType = encryptType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public EncryptType getEncryptType() {
        return encryptType;
    }

    /** 加密类型. */
    public enum EncryptType {
        ID,
        ID_CLEAN,
        PHONE,
        PHONE_CLEAN,
        CREDIT,
        CREDIT_CLEAN,
        ADDRESS,
        ADDRESS_CLEAN,
    }
}
