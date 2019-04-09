package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.PropertiesUtil;

import java.util.Arrays;

/**
 * @ClassName: HBaseConsumer
 * @Description: ${description}
 * @Author: xy
 * @Date: 2019/4/9 21:18
 * @Version: 1.0
 */
public class HBaseConsumer {
    public static void main(String[] args) {
        KafkaConsumer<String,String> kafkaconsumer=new KafkaConsumer<>(PropertiesUtil.properties);
        kafkaconsumer.subscribe(Arrays.asList(PropertiesUtil.getProperty("kafka.topics")));

        while(true){
            ConsumerRecords<String, String> records = kafkaconsumer.poll(100);
            for(ConsumerRecord<String, String> cr:records){
                String orivalue=cr.value();
                System.out.println(orivalue);
            }
        }

    }
}
