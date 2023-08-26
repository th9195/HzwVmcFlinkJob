package com.hzw.fdc.function.online.MainFabWindow;

import com.hzw.fdc.bean.ContextConfig;
import com.hzw.fdc.bean.OracleConfig;
import com.hzw.fdc.util.ProjectConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


/**
 * @author lwt
 * job启动初始化抓取context_config
 */
public class InitFetchContextConfig {

    private static Logger logger = LoggerFactory.getLogger(InitFetchContextConfig.class);
    private static final String CONTEXT_INFO_SQL = "select * from V_CONTEXT_INFO";

    public static OracleConfig<List<ContextConfig>> fetch() throws Exception {
        Connection conn = null;
        Statement selectStmt = null;
        ResultSet resultSet = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection(ProjectConfig.MAIN_FAB_CORE_ORACLE_URL(), ProjectConfig.MAIN_FAB_CORE_ORACLE_USER(), ProjectConfig.MAIN_FAB_CORE_ORACLE_PASSWORD());
            List<ContextConfig> contextConfigList = new ArrayList<>();
            selectStmt = conn.createStatement();
            resultSet = selectStmt.executeQuery(CONTEXT_INFO_SQL);
            while (resultSet.next()) {
                ContextConfig config = new ContextConfig()
                        .setLocationId(resultSet.getLong("LOCATION_ID"))
                        .setLocationName(resultSet.getString("LOCATION_NAME"))
                        .setModuleId(resultSet.getLong("MODULE_ID"))
                        .setModuleName(resultSet.getString("MODULE_NAME"))
                        .setToolName(resultSet.getString("TOOL_NAME"))
                        .setChamberName(resultSet.getString("CHAMBER_NAME"))
                        .setRecipeName(resultSet.getString("RECIPE_NAME"))
                        .setProductName(resultSet.getString("PRODUCT_NAME"))
                        .setStage(resultSet.getString("STAGE_NAME"))
                        .setContextId(resultSet.getLong("CONTEXT_ID"))
                        .setToolGroupId(resultSet.getLong("TOOL_GROUP_ID"))
                        .setToolGroupName(resultSet.getString("TOOL_GROUP_NAME"))
                        .setToolGroupVersion(resultSet.getLong("TOOL_GROUP_VERSION"))
                        .setChamberGroupId(resultSet.getLong("CHAMBER_GROUP_ID"))
                        .setChamberGroupName(resultSet.getString("CHAMBER_GROUP_NAME"))
                        .setChamberGroupVersion(resultSet.getLong("CHAMBER_GROUP_VERSION"))
                        .setRecipeGroupId(resultSet.getLong("RECIPE_GROUP_ID"))
                        .setRecipeGroupName(resultSet.getString("RECIPE_GROUP_NAME"))
                        .setRecipeGroupVersion(resultSet.getLong("RECIPE_GROUP_VERSION"))
                        .setLimitStatus(resultSet.getBoolean("LIMIT_STATUS"))
                        .setArea(resultSet.getString("AREA"))
                        .setSection(resultSet.getString("SECTION"))
                        .setMesChamberName(resultSet.getString("MES_CHAMBER_NAME"))
                        ;
                contextConfigList.add(config);
            }

            return new OracleConfig<List<ContextConfig>>().setDataType("context")
                    .setStatus(true)
                    .setDatas(contextConfigList);
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
