package com.hzw.fdc.engine.controlwindow.data.impl;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
@Builder
public class WindowClipResponse implements Serializable {
    public static final String SUCCESS_CODE = "0000";
    public static final String COMMON_FAIL_CODE = "1000";
    public static final String PARENT_FAIL_CODE = "1100";
    public static final String CHILDREN_FAIL_CODE = "1200";
    private String msgCode;
    private String msg;
    private List<WindowSplitInfo> windowTimeRangeList;

    @Data
    @Builder
    public static class WindowSplitInfo {
        private Long windowId;
        private Long startTime;
        private Boolean startInclude;
        private Long endTime;
        private Boolean endInclude;
    }
}
