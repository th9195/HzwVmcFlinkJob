package com.hzw.fdc.util.hbaseUtils;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

/**
 * HbaseUtilJava
 *
 * @author tobytang
 * @desc:
 * @date 2022/10/24 10:38
 * @update 2022/10/24 10:38
 * @since 1.0.0
 *
 * 在工厂环境 根据 条件生产rowkey
 * 案例 :
 * java -classpath MainFabFlinkJob-v1.3dev-97365c3-221024104058.jar  com.hzw.fdc.util.hbaseUtils.HbaseUtilJava --tool Tom --chamber hui --start 1666577975000 --end 1666577975000
 **/
public class HbaseUtilJava {


    public static void main(String[] args) throws Exception {
//        HbaseUtil hbaseUtil = new HbaseUtil();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String type = parameterTool.get("type","getRowkey");

        switch (type){
            case "getRowkey" :
                getRowkey(parameterTool);
                break;

            default:
                System.out.println("Please input you type");
        }


//        close();
    }


    /**
     * 获取rowkey
     * @param parameterTool
     */
    private static void getRowkey(ParameterTool parameterTool) {

        String toolName = "BOXEY01";
        String chamberName = "CHA";
        String indicatorId = "18131";
        Long startTime = 1666317600000l;
        Long endTime = 1666321200000l;
        toolName = parameterTool.get("tool",toolName);
        chamberName= parameterTool.get("chamber",chamberName);

        indicatorId = parameterTool.get("indicatorId",indicatorId);

        startTime = Long.parseLong(parameterTool.get("start",startTime.toString()));
        endTime = Long.parseLong(parameterTool.get("end",endTime.toString()));

        System.out.println("toolName == " + toolName + "; " +
                "chamberName == " + chamberName + "; " +
                "indicatorId == " + indicatorId + "; " +
                "startTime == " + startTime + "; " +
                "endTime == " + endTime + "; " );

        String indicatorSpecialID = toolName + chamberName + indicatorId;
        String slatKey = StringUtils.leftPad(Integer.toString(Math.abs(indicatorSpecialID.hashCode() % 10000)), 4, '0');

        String indicatorSpecialMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes(indicatorSpecialID));


        Long startReverseTime = Long.MAX_VALUE - startTime;
        Long endReverseTime = Long.MAX_VALUE - endTime;

        String startRowkey = slatKey + "|" + indicatorSpecialMd5 + "|" + endReverseTime;
        String endRowkey = slatKey + "|" + indicatorSpecialMd5 + "|" + startReverseTime;

        System.out.println("startRowkey == " + startRowkey);
        System.out.println("endRowkey == " + endRowkey);

    }
}
