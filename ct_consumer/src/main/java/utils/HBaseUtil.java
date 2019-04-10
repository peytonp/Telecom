package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * @ClassName: HBaseUtil
 * @Description:
 * 1、namespace 命名空间
 * 2、createTable 创建表
 * 3、isTable 判断表是否存在
 * 4、Region、RowKey、分区键
 * @Author: xy
 * @Date: 2019/4/9 22:26
 * @Version: 1.0
 */
public class HBaseUtil {

    /**
     * @FunctionName: initNameSpace
     * @Description: 初始化命名空间
     * @Author: xy
     * @Date: 2019/4/9 22:35
     * @Version: 1.0
     * @Param: [conf, namespace]，[配置，命名空间]
     * @Return: void
     */
    public static void initNameSpace(Configuration conf,String namespace) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        //命名空间描述器
        NamespaceDescriptor nd = NamespaceDescriptor
                .create(namespace)
                .addConfiguration("AUTHOR", "xy")
                .build();
        //通过admin对象来创建命名空间
        admin.createNamespace(nd);
        //关闭两个对象
        close(admin,connection);
    }

    /**
     * @FunctionName: close
     * @Description: 关闭admin对象和connection对象
     * @Author: xy
     * @Date: 2019/4/9 22:34
     * @Version: 1.0
     * @Param: [admin, connection]
     * @Return: void
     */
    public static void close(Admin admin,Connection connection) throws IOException {
        if(admin!=null){
            admin.close();
        }
        if(connection!=null){
            connection.close();
        }
    }

    /**
     * @FunctionName: createTable
     * @Description: 创建HBase的表
     * @Author: xy
     * @Date: 2019/4/9 22:39
     * @Version: 1.0
     * @Param: [conf, tableName, regions, columnFamily] [配置，表名，分区，列族]
     * @Return: void
     */
    public static void createTable(Configuration conf,String tableName,int regions,String...columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        //判断表是否存在
        if(isExistTable(conf,tableName)){
            return;
        }

        //表空间描述器 HTableDescriptor
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        for(String cf:columnFamily){
            //列描述器 HColumnDescriptor
            htd.addFamily(new HColumnDescriptor(cf));
        }
        htd.addCoprocessor("hbase.CalleeWriteObserver");
        //创建表，分区键
        admin.createTable(htd,genSplitKeys(regions));
        //关闭对象
        close(admin,connection);
    }

    /**
     * @FunctionName: genSplitKeys
     * @Description: 分区键
     * @Author: xy
     * @Date: 2019/4/9 22:57
     * @Version: 1.0
     * @Param: [regions]
     * @Return: byte[][]
     */
    private static byte[][] genSplitKeys(int regions){
        //存放分区键的数组
        String[] keys = new String[regions];
        //格式化分区键的形式
        DecimalFormat df = new DecimalFormat("00");

        for(int i=0;i<regions;i++){
            keys[i]=df.format(i)+"|";
        }

        byte[][] splitKeys = new byte[regions][];
        //排序 保证分区键是有序的
        TreeSet<byte[]> treeSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for(int i=0;i<regions;i++){
            treeSet.add(Bytes.toBytes(keys[i]));
        }

        //迭代输出
        Iterator<byte[]> iterator = treeSet.iterator();
        int index=0;
        while(iterator.hasNext()){
            byte[] next = iterator.next();
            splitKeys[index++]=next;
        }
        return splitKeys;
    }

    /**
     * @FunctionName: isExistTable
     * @Description: 判断表是否存在
     * @Author: xy
     * @Date: 2019/4/9 22:40
     * @Version: 1.0
     * @Param: [conf, tableName] [配置，表名]
     * @Return: void
     */
    public static boolean isExistTable(Configuration conf,String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        boolean result = admin.tableExists(TableName.valueOf(tableName));
        close(admin,connection);
        return result;
    }

    /**
     * @FunctionName: genRowkey
     * @Description: 生成rowkey:regionCode_caller_buildTime_callee_flag_duration
     * @Author: xy
     * @Date: 2019/4/10 19:30
     * @Version: 1.0
     * @Param: [regioncode, caller, buildTime, callee, flag, duration]
     * [散列的键、主叫、建立时间、被叫、标记主被叫、通话持续时间]
     * @Return: java.lang.String
     */
    public static String genRowkey(String regionCode,String caller,String buildTime,String callee,String flag,String duration){
        StringBuilder sb = new StringBuilder();
        sb.append(regionCode+"_")
                .append(caller+"_")
                .append(buildTime+"_")
                .append(callee+"_")
                .append(flag+"_")
                .append(duration);
        return sb.toString();
    }


    /**
     * @FunctionName: genRegionCode
     * @Description: 生成分区号
     * @Author: xy
     * @Date: 2019/4/10 19:46
     * @Version: 1.0
     * @Param: [caller, buildTime, regions]
     * [主叫，通话建立时间，region个数]
     * @Return: java.lang.String 返回分区号
     */
    public static String genRegionCode(String caller,String buildTime,int regions){
        int len=caller.length();
        //取出主叫后四位
        String lastPhone = caller.substring(len - 4);
        //取出年和月
        String ym = buildTime.replaceAll("-", "")
                .replaceAll(" ", "")
                .replaceAll(":", "")
                .substring(0, 6);

        //离散操作1
        Integer x = Integer.valueOf(lastPhone) ^ Integer.valueOf(ym);
        //离散操作2
        int y = x.hashCode();
        //生成分区号
        int regionCode = y % regions;
        //格式化分区号
        DecimalFormat df = new DecimalFormat("00");
        return df.format(regionCode);

    }

}
