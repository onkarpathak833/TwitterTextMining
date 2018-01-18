package Project1.TwitterStreaming;

import java.util.List;
import java.util.Properties;

//import kafka.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import kafka.utils.ShutdownableThread;
public class TwitterKafkaConsumer extends ShutdownableThread {
	
	 private final KafkaConsumer<String, String> consumer;
	    private final String topic;
	    
	    public static final String KAFKA_SERVER_URL = "18.220.10.232";
	    public static final int KAFKA_SERVER_PORT = 9092;
	    public static final String CLIENT_ID = "camus";
	 
	    public TwitterKafkaConsumer(String topic) {
	        super("KafkaConsumerExample", false);
	        Properties props = new Properties();
	        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
	        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
	        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
	        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
	        props.put(ConsumerConfig.SESSION_TIMEOUT_MS, "30000");
	        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "range");
	        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	 
	        consumer = new KafkaConsumer<String, String>(props);
	        this.topic = topic;
	    }
	 
	    @Override
	    public void doWork() {
	    	consumer.subscribe(this.topic);
	        //consumer.subscribe(Collections.singletonList(this.topic));
	        ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(50000);
	        if(records!=null){
	        List<ConsumerRecord<String, String>> consumerRecords = records.records();
	        //System.out.println(records.topic());
	        //System.out.println("Records : "+records.toString());
	        for (ConsumerRecord<String, String> record : consumerRecords) {
	            try {
					System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
	    }
	    }
	 
	    @Override
	    public String name() {
	        return null;
	    }
	 
	    @Override
	    public boolean isInterruptible() {
	        return false;
	    }
	
	    public static void main(String[] args) {
	    	TwitterKafkaConsumer consumerThread = new TwitterKafkaConsumer("TEST_TOPIC");
	        consumerThread.start();
	    }
	
	
}
