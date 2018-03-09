import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer extends Thread {

    private final KafkaProducer<Integer,String> producer;
    private final String topic;
    private final boolean isAsync;

    public Producer(String topic, boolean isAsync) {
        Properties props=new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        this.topic = topic;
        this.isAsync = isAsync;
    }


    public void run() {
        int messageno=1;
        while (true){
            String messageStr = "Message_" + messageno;
            long startTime = System.currentTimeMillis();
            if(isAsync){
                producer.send(new ProducerRecord<Integer, String>(topic,messageno,messageStr),new DemoCallBack(startTime, messageno, messageStr));
            }else {
                try {
                    producer.send(new ProducerRecord<Integer, String>(topic,messageno,messageStr)).get();
                } catch (InterruptedException |ExecutionException e) {
                    e.printStackTrace();
                } 
            }
        }
    }


}

class DemoCallBack implements Callback {

    private final long startTime;
    private final int key;
    private final String message;
    public DemoCallBack(long startTime1, int key, String message) {
        startTime = startTime1;
        this.key = key;
        this.message = message;
    }
    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (recordMetadata != null) {
            System.out.println(
                    "message(" + key + ", " + message + ") sent to partition(" + recordMetadata.partition() +
                            "), " +
                            "offset(" + recordMetadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            e.printStackTrace();
        }

    }
}
