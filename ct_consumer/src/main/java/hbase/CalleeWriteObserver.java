package hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import utils.HBaseUtil;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @ClassName: CalleeWriteObserver
 * @Description: 协处理器，类似于触发器
 * @Author: xy
 * @Date: 2019/4/10 23:26
 * @Version: 1.0
 */
public class CalleeWriteObserver extends BaseRegionObserver {
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        //获取你想要操作的那张目标表
        String targetTableName = PropertiesUtil.getProperty("hbase.calllog.tablename");

        //获取当前put数据的表
        String currentTableName = e.getEnvironment().getRegionInfo().getTable().getNameAsString();

        if(targetTableName!=currentTableName) {
            return;
        }

        String oriRowKey = Bytes.toString(put.getRow());
        String[] splitOriRowKey = oriRowKey.split("_");
        String caller=splitOriRowKey[1];
        String callee=splitOriRowKey[3];
        String buildTime=splitOriRowKey[2];
        if(splitOriRowKey[4].equals("0")){
            return ;
        }
        String flag = "0";

        String duration=splitOriRowKey[5];

        Integer regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));
        String regionCode = HBaseUtil.genRegionCode(caller, buildTime, regions);
        String calleeRowKey = HBaseUtil.genRowkey(regionCode, callee, buildTime, caller, flag, duration);
        String buildTimeTs="";
        try {
            buildTimeTs = String.valueOf(sdf.parse(buildTime).getTime());
        } catch (ParseException e1) {
            e1.printStackTrace();
        }

        Put calleePut = new Put(Bytes.toBytes(calleeRowKey));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("callee"), Bytes.toBytes(callee));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("caller"), Bytes.toBytes(caller));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("build_time"), Bytes.toBytes(buildTime));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("build_time_ts"), Bytes.toBytes(buildTimeTs));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes(flag));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        //HTable table = (HTable) e.getEnvironment().getTable(TableName.valueOf(targetTableName));
        Table table = e.getEnvironment().getTable(TableName.valueOf(targetTableName));
        table.put(calleePut);
        table.close();
    }
}
