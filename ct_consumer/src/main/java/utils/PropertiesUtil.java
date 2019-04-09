package utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @ClassName: PropertiesUtil
 * @Description: ${description}
 * @Author: xy
 * @Date: 2019/4/9 21:05
 * @Version: 1.0
 */
public class PropertiesUtil {
    public static Properties properties =new Properties();
    static{
        //获取配置文件、方便维护
        InputStream is = ClassLoader.getSystemResourceAsStream("hbase_consumer.properties");
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @FunctionName: getProperty
     * @Description: 获取参数值
     * @Author: xy
     * @Date: 2019/4/9 21:12
     * @Version: 1.0
     * @Param: [key]
     * @Return: java.lang.String
     */
    public static String getProperty(String key){
        return properties.getProperty(key);
    }
}
