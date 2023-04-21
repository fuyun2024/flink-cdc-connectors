package com.ververica.cdc.connectors.sf.entity;

import java.io.Serializable;
import java.util.Objects;

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

    @Override
    public String toString() {
        return "EncryptField{"
                + "fieldName='"
                + fieldName
                + '\''
                + ", encryptType="
                + encryptType
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EncryptField that = (EncryptField) o;
        return Objects.equals(fieldName, that.fieldName) && encryptType == that.encryptType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, encryptType);
    }
}
