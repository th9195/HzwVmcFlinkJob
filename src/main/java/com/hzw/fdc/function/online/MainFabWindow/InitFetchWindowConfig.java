package com.hzw.fdc.function.online.MainFabWindow;

import com.hzw.fdc.bean.OracleConfig;
import com.hzw.fdc.bean.WindowConfig;
import com.hzw.fdc.bean.po.WindowConfigPO;
import com.hzw.fdc.util.ProjectConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author lwt
 * job启动初始化抓取context_config
 */
public class InitFetchWindowConfig {

    private static Logger logger = LoggerFactory.getLogger(InitFetchWindowConfig.class);
    private static final String CONTEXT_INFO_SQL = "select * from V_WINDOW_INFO";

    public static OracleConfig<List<WindowConfig>> fetch() throws Exception {
        Connection conn = null;
        Statement selectStmt = null;
        ResultSet resultSet = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection(ProjectConfig.MAIN_FAB_CORE_ORACLE_URL(), ProjectConfig.MAIN_FAB_CORE_ORACLE_USER(), ProjectConfig.MAIN_FAB_CORE_ORACLE_PASSWORD());
            List<WindowConfigPO> windowPoList = new ArrayList<>();
            selectStmt = conn.createStatement();
            resultSet = selectStmt.executeQuery(CONTEXT_INFO_SQL);
            while (resultSet.next()) {
                WindowConfigPO config = new WindowConfigPO()
                        .setContextId(resultSet.getLong("CONTEXT_ID"))
                        .setControlWindowId(resultSet.getLong("CONTROL_WINDOW_ID"))
                        .setParentWindowId(resultSet.getLong("PARENT_WINDOW_ID"))
                        .setControlWindowType(resultSet.getString("CONTROL_WINDOW_TYPE"))
                        .setIsTopWindows(resultSet.getBoolean("IS_TOP_WINDOWS"))
                        .setIsConfiguredIndicator(resultSet.getBoolean("IS_CONFIGURED_INDICATOR"))
                        .setWindowStart(resultSet.getString("WINDOW_START"))
                        .setWindowEnd(resultSet.getString("WINDOW_END"))
                        .setControlPlanId(resultSet.getLong("CONTROL_PLAN_ID"))
                        .setControlPlanVersion(resultSet.getLong("CONTROL_PLAN_VERSION"))
                        .setSensorAliasId(resultSet.getLong("SENSOR_ALIAS_ID"))
                        .setSensorAliasName(resultSet.getString("SENSOR_ALIAS_NAME"))
                        .setIndicatorId(resultSet.getLong("INDICATOR_ID"))
                        .setWindowExt(resultSet.getString("WINDOW_EXT"))
                        .setCalculationTrigger(resultSet.getString("CALC_TRIGGER"));
                windowPoList.add(config);

            }
            List<WindowConfig> result = new ArrayList<>();
            Map<String, List<WindowConfigPO>> map = windowPoList.stream().collect(Collectors.groupingBy(po -> po.getContextId() + "/" + po.getControlPlanId() + "/" + po.getControlWindowId()));
            for (List<WindowConfigPO> pos : map.values()) {
                WindowConfigPO one = pos.get(0);
                WindowConfig windowConfig = new WindowConfig()
                        .setContextId(one.getContextId())
                        .setControlWindowId(one.getControlWindowId())
                        .setParentWindowId(one.getParentWindowId())
                        .setControlWindowType(one.getControlWindowType())
                        .setIsTopWindows(one.getIsTopWindows())
                        .setIsConfiguredIndicator(one.getIsConfiguredIndicator())
                        .setWindowStart(one.getWindowStart())
                        .setWindowEnd(one.getWindowEnd())
                        .setControlPlanId(one.getControlPlanId())
                        .setControlPlanVersion(one.getControlPlanVersion())
                        .setCalculationTrigger(one.getCalculationTrigger());
                if ("CycleWindowMax".equals(windowConfig.getControlWindowType()) || "CycleWindowMin".equals(windowConfig.getControlWindowType())) {
                    windowConfig.setWindowEnd(windowConfig.getWindowEnd() + "_" + one.getWindowExt());
                }
                List<WindowConfig.SensorAlias> aliasList = new ArrayList<>();
                if (one.getIsConfiguredIndicator()) {
                    Map<Long, List<WindowConfigPO>> aliasMap = pos.stream().filter(x -> x.getSensorAliasId() != null).collect(Collectors.groupingBy(WindowConfigPO::getSensorAliasId));
                    for (List<WindowConfigPO> aliasPo : aliasMap.values()) {
                        WindowConfigPO po = aliasPo.get(0);
                        aliasList.add(new WindowConfig.SensorAlias()
                                .setSensorAliasId(po.getSensorAliasId())
                                .setSensorAliasName(po.getSensorAliasName())
                                .setIndicatorId(aliasPo.stream().map(WindowConfigPO::getIndicatorId).collect(Collectors.toList())));
                    }
                }
                windowConfig.setSensorAlias(aliasList);
                result.add(windowConfig);
            }
            return new OracleConfig<List<WindowConfig>>().setDataType("runwindow")
                    .setStatus(true)
                    .setDatas(result);
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (selectStmt != null) {
                selectStmt.close();
            }
            if (conn != null) {
                conn.close();
                logger.warn("关闭JDBC连接!");
            } else {
                logger.error("JDBC连接初始化失败!");
            }
        }
    }

}
