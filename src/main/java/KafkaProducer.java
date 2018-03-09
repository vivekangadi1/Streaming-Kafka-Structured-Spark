import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducer {

    public static void main(String[] args) {
        Properties prop=new Properties();
        prop.setProperty("bootstrap.servers","192.168.99.100:9092");
        prop.setProperty("key.serializer", StringSerializer.class.getName());
        prop.setProperty("value.serializer",StringSerializer.class.getName()   );
        prop.setProperty("acks","-1");
        prop.setProperty("retries","3");
        prop.setProperty("linger.ms","1");

        Producer<String,String> p1=new org.apache.kafka.clients.producer.KafkaProducer<String, String>(prop);

        for(int i=0;i<90;i++){
            ProducerRecord<String,String> producerRecord=new ProducerRecord<String, String>("cantona",Integer.toString(i),"The King" +
                    "of Manchester United"+Integer.toString(i));
            p1.send(producerRecord);
        }
        p1.close();
    }
}
