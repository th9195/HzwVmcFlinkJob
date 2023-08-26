package com.hzw.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.IOException;
import java.util.Scanner;

/**
 * HbaseUtil
 *
 * @author tobytang
 * @desc:
 * @date 2022/8/11 9:21
 * @update 2022/8/11 9:21
 * @since 1.0.0
 **/
public class HbaseUtil {

    public static Admin admin = null;
    public static Connection conn = null;

    public static String tableName = "mainfab_indicatordata_table";

    public HbaseUtil() {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","mainfab01:2181,mainfab02:2181,mainfab03:2181");
//        conf.set("hbase.rootdir", "hdfs://h71:9000/hbase");
        try {
            // todo 创建hbase的连接对象
            conn = ConnectionFactory.createConnection(conf);

            // todo 根据连接对象, 获取相关的管理对象: admin (跟表相关的操作)  Table(跟表数据相关的操作)
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭资源
     * @throws Exception
     */
    public static void close() throws Exception {
        //5. 释放资源

        if(null != admin) admin.close();
        if(null != conn) conn.close();
    }

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

    private static void getRowkey(ParameterTool parameterTool) {

        String toolName = parameterTool.get("tool","");
        String chamberName = parameterTool.get("chamber","");
        String indicatorId = parameterTool.get("indicatorId","");

        Long startTime = Long.parseLong(parameterTool.get("start","0"));
        Long endTime = Long.parseLong(parameterTool.get("end","0"));

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


    private void rowFilter(String tableName) throws IOException {

        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();

        String toolName = "tom_tool_01";
        String chamberName = "tom_chamber_01";
        Long indicatorId = 37143l;

        String indicatorSpecialID = toolName + chamberName + indicatorId;
        String slatKey = StringUtils.leftPad(Integer.toString(Math.abs(indicatorSpecialID.hashCode() % 10000)), 4, '0');

        String indicatorSpecialMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes(indicatorSpecialID));


//        Long startTimestatmp = 1660097632000l;
//        Long startTimestatmp = 1652235232000l;
        Long startTimestatmp = 1640966400000l;
        Long endTimestatmp = 1660270432000l;
        Long startReverseTime = Long.MAX_VALUE - startTimestatmp;
        Long endReverseTime = Long.MAX_VALUE - endTimestatmp;


        String startRowkey = slatKey + "|" + indicatorSpecialMd5 + "|" + endReverseTime;
        String endRowkey = slatKey + "|" + indicatorSpecialMd5 + "|" + startReverseTime;

        System.out.println("startRowkey == " + startRowkey);
        System.out.println("endRowkey == " + endRowkey);

        scan.setStartRow(Bytes.toBytes(startRowkey));
        scan.setStopRow(Bytes.toBytes(endRowkey));

        scan.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("INDICATOR_ID"));
        scan.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("TOOL_ID"));
        scan.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("CHAMBER_ID"));
        scan.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("INDICATOR_VALUE"));
        scan.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("PRODUCT_ID"));
        scan.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("STAGE"));


        ResultScanner scanner = table.getScanner(scan);
        int num = 0;
        for (Result r : scanner) {
            System.out.println("--------------------------------------"+ ++num +"-------------------------------------");
            for (Cell cell : r.rawCells()) {
                System.out.println(
                        "Rowkey-->"+Bytes.toString(r.getRow())+"  "+
                                "Familiy:Quilifier-->"+Bytes.toString(CellUtil.cloneQualifier(cell))+"  "+
                                "Value-->"+Bytes.toString(CellUtil.cloneValue(cell)));
            }

        }

    }


    /**
     * rowkey == 7394|99e5bc25aa63e9fa3dbba5639a16322f|9223370376670625796
     * @param tableName
     * @throws Exception
     */
    public void dependentColumnFilter(String tableName) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();

        Filter filter = new DependentColumnFilter(Bytes.toBytes("f1"), Bytes.toBytes("INDICATOR_ID"),false, CompareFilter.CompareOp.EQUAL,new LongComparator(37143l));

        scan.setFilter(filter);
        scan.setTimeRange(1660097632000l,1660270432000l);
//        scan.setTimeRange(1660097632000l,1660270432000l);
//        scan.setTimeRange(1652235232000l,1660184259248l); // 2022-05-11
//        scan.setTimeRange(1660184012246l,1660184259248l); //


        ResultScanner scanner = table.getScanner(scan);
        int num = 0;
        for (Result r : scanner) {
            System.out.println("--------------------------------------"+ ++num +"-------------------------------------");
            for (Cell cell : r.rawCells()) {
                System.out.println(
                        "Rowkey-->"+Bytes.toString(r.getRow())+"  "+
                                "Familiy:Quilifier-->"+Bytes.toString(CellUtil.cloneQualifier(cell))+"  "+
                                "Value-->"+Bytes.toString(CellUtil.cloneValue(cell)));
            }

        }
    }

    public void familyFilter(String tableName) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        Filter filter = new FamilyFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("grc")));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);

        for (Result r : scanner) {
            for (Cell cell : r.rawCells()) {
                System.out.println(
                        "Rowkey-->"+Bytes.toString(r.getRow())+"  "+
                                "Familiy:Quilifier-->"+Bytes.toString(CellUtil.cloneQualifier(cell))+"  "+
                                "Value-->"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }


    /**
     * 创建表
     * @throws Exception
     */
    public static void testCreateTable() throws Exception {

        //todo 1 执行相关的操作 : 创建表
        //首先判断表是否存在
        boolean flag = admin.tableExists(TableName.valueOf("testCreateTable")); // 存在 返回true

        System.out.println("flag == " + flag);
        if(!flag){ // 说明表不存在
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf("testCreateTable"));
            table.setDurability(Durability.SYNC_WAL);
            //创建列簇
            HColumnDescriptor hcd = new HColumnDescriptor("f1");
            hcd.setCompressTags(false);
            hcd.setMaxVersions(3);

            table.addFamily(hcd);

            //执行创建表
            admin.createTable(table);
        }
    }
}
