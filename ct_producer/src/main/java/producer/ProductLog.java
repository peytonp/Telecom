package producer;

import java.io.*;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @ClassName: ProductLog
 * @Description: ${description}
 * @Author: xy
 * @Date: 2019/4/3 22:03
 * @Version: 1.0
 */
public class ProductLog {
    private String startTime="2019-01-01";
    private String endTime="2019-12-31";

    /**
     * 用于存放电话号码 和电话号码+姓名
     */
    private List<String> phoneList=new ArrayList<String>();

    private Map<String,String> phoneNameMap=new HashMap<>();

    public void initPhone(){
        phoneList.add("17078388295");
        phoneList.add("13980337439");
        phoneList.add("14575535933");
        phoneList.add("19902496992");
        phoneList.add("18549641558");
        phoneList.add("17005930322");
        phoneList.add("18468618874");
        phoneList.add("18576581848");
        phoneList.add("15978226424");
        phoneList.add("15542823911");
        phoneList.add("17526304161");
        phoneList.add("15422018558");
        phoneList.add("17269452013");
        phoneList.add("17764278604");
        phoneList.add("15711910344");
        phoneList.add("15714728273");
        phoneList.add("16061028454");
        phoneList.add("16264433631");
        phoneList.add("17601615878");
        phoneList.add("15897468949");

        phoneNameMap.put("17078388295", "李雁");
        phoneNameMap.put("13980337439", "卫艺");
        phoneNameMap.put("14575535933", "仰莉");
        phoneNameMap.put("19902496992", "陶欣悦");
        phoneNameMap.put("18549641558", "施梅梅");
        phoneNameMap.put("17005930322", "金虹霖");
        phoneNameMap.put("18468618874", "魏明艳");
        phoneNameMap.put("18576581848", "华贞");
        phoneNameMap.put("15978226424", "华啟倩");
        phoneNameMap.put("15542823911", "仲采绿");
        phoneNameMap.put("17526304161", "卫丹");
        phoneNameMap.put("15422018558", "戚丽红");
        phoneNameMap.put("17269452013", "何翠柔");
        phoneNameMap.put("17764278604", "钱溶艳");
        phoneNameMap.put("15711910344", "钱琳");
        phoneNameMap.put("15714728273", "缪静欣");
        phoneNameMap.put("16061028454", "焦秋菊");
        phoneNameMap.put("16264433631", "吕访琴");
        phoneNameMap.put("17601615878", "沈丹");
        phoneNameMap.put("15897468949", "褚美丽");
    }

    /**
     * @FunctionName: product
     * @Description: 数据形式：主叫，被叫，通话建立时间，通话持续时间
     * 15837312345,13737312345,2017-01-09 08:09:10,0360
     * 对应字段名：caller,callee,buildTime,duration
     * @Author: xy
     * @Date: 2019/4/3 22:21
     * @Version: 1.0
     * @Param: []
     * @Return: java.lang.String
     */
    public String product() {
        //主被叫电话号
        String caller = null;
        String callee = null;
        //主被叫姓名
        String callerName = null;
        String calleeName = null;

        //取得主叫号码
        int callerIndex = (int) (Math.random() * phoneList.size());
        caller = phoneList.get(callerIndex);
        callerName = phoneNameMap.get(caller);

        while(true){
            //取得被叫号码
            int calleeIndex = (int) (Math.random() * phoneList.size());
            callee = phoneList.get(calleeIndex);
            calleeName = phoneNameMap.get(callee);
            if (calleeIndex != callerIndex){
                break;
            }
        }

        //随机通话建立时间
        String buildTime=randomBuildTime(startTime,endTime);
        //通话时长
        DecimalFormat df=new DecimalFormat("0000");
        String duration = df.format(new Random().nextInt(30*60));

        StringBuilder stringBuilder=new StringBuilder();
        stringBuilder.append(caller+",").append(callee+",").append(buildTime+",").append(duration);

        return stringBuilder.toString();
    }

    /**
     * @FunctionName: randomBuildTime
     * @Description: 随机生成通话建立时间
     * @Author: xy
     * @Date: 2019/4/3 23:10
     * @Version: 1.0
     * @Param: [startTime, endTime]
     * @Return: java.lang.String
     */
    private String randomBuildTime(String startTime,String endTime) {
        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd");
        Date startDate=null;
        Date endDate=null;
        try {
            startDate=simpleDateFormat.parse(startTime);
            endDate= simpleDateFormat.parse(endTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if(endDate.getTime()<=startDate.getTime()){
            return null;
        }

        //随机通话建立时间的Long型
        long randomTS=startDate.getTime()+(long)((endDate.getTime()-startDate.getTime())*Math.random());
        Date resultDate=new Date(randomTS);
        SimpleDateFormat sf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String resultTimeString=sf.format(resultDate);
        return resultTimeString;
    }

   /**
     * @FunctionName: writeLog
     * @Description: 写数据到文件中
     * @Author: xy
     * @Date: 2019/4/3 23:21
     * @Version: 1.0
     * @Param: [filePath]
     * @Return: void
     */
    public void writeLog(String filePath){
        try {
            OutputStreamWriter osw=new OutputStreamWriter(new FileOutputStream(filePath,true));
            while(true){
                Thread.sleep(5);
                String log=product();
                System.out.println(log);
                osw.write(log+"\n");
                osw.flush();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        args=new String[]{"H:\\项目实战\\caller.csv"};

        if(args == null|| args.length<=0){
            System.out.println("没这个路径");
            return;
        }

        ProductLog productLog=new ProductLog();
        productLog.initPhone();
        productLog.writeLog(args[0]);

    }

}
