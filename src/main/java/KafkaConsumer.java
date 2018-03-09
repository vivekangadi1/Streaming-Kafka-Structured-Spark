

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumer {

    public static void main(String[] args) {
        Properties prop=new Properties();
        prop.setProperty("bootstrap.servers","192.168.99.100:9092");
        prop.setProperty("key.deserializer", StringDeserializer.class.getName());
        prop.setProperty("value.deserializer",StringDeserializer.class.getName());
        prop.setProperty("group.id","giggs");
        prop.setProperty("enable.auto.commit","false");
//        prop.setProperty("auto.commit.interval.ms","1000");
        prop.setProperty("auto.offset.reset","earliest");

        Consumer<String,String> consumer=new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(prop);
        consumer.subscribe(Arrays.asList("cantona"));

        while (true){
            ConsumerRecords<String,String> consumerRecord=consumer.poll(100);
            for(ConsumerRecord<String,String> consumerRecords:consumerRecord){
                System.out.println("value"+ consumerRecords.value()+"heyyyyy");
            }
            consumer.commitSync();

        }
    }
}
