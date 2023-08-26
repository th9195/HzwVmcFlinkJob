package com.hzw.fdc.bean;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * @author lwt
 */
@Data
@Accessors(chain = true)
public class OracleConfig<T> {

    private String dataType;
    private Boolean status;
    private T datas;

    public String getDataType() {
        return dataType;
    }

    public Boolean getStatus() {
        return status;
    }

    public T getDatas() {
        return datas;
    }
}
