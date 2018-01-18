package Project1.TwitterStreaming;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerConnector;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class KafkaConsumer {
	
	 public ConsumerConnector consumerConnector = null;
	    public final String topic = "2GScam";
	 
	    public void initialize() {
	          Properties props = new Properties();
	          props.put("zookeeper.connect", "18.220.10.232:2181/kafka");
	          props.put("group.id", "testgroup");
	          props.put("zookeeper.session.timeout.ms", "400000");
	          props.put("zookeeper.sync.time.ms", "300");
	          props.put("auto.commit.interval.ms", "100");
	          ConsumerConfig conConfig = new ConsumerConfig(props);
	          consumerConnector = (ConsumerConnector) Consumer.createJavaConsumerConnector(conConfig);
	    }
	 
	    public void consume() {
	    	 scala.collection.Map<String, Object> topicCount = (scala.collection.Map<String, Object>) new HashMap<String,Integer>();       
	          ((Hashtable<Object, Object>) topicCount).put(topic, new Integer(1));
	         
	          //ConsumerConnector creates the message stream for each topic
	         Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = (Map<String, List<KafkaStream<byte[], byte[]>>>) consumerConnector.createMessageStreams(topicCount); 
	         Map<String,Object> test = new HashMap<String, Object>();
	        // consumerConnector.createMessageStreams(test);
	         
	          // Get Kafka stream for topic 'mytopic'
	          List<KafkaStream<byte[], byte[]>> kStreamList = consumerStreams.get(topic);
	                                               
	          // Iterate stream using ConsumerIterator
	          for (final KafkaStream<byte[], byte[]> kStreams : kStreamList) {
	                 ConsumerIterator<byte[], byte[]> consumerIte = kStreams.iterator();
	                
	                 while (consumerIte.hasNext())
	                        System.out.println("Message consumed from topic[" + topic + "] : "       +
	                                        new String(consumerIte.next().message()));              
	          }
	          //Shutdown the consumer connector
	          if (consumerConnector != null)   consumerConnector.shutdown();          
	    }
	 
	    public static void main(String[] args) throws InterruptedException {
	          KafkaConsumer kafkaConsumer = new KafkaConsumer();
	          // Configure Kafka consumer
	          kafkaConsumer.initialize();
	          // Start consumption
	          kafkaConsumer.consume();
	    }
	
	
	
	
}
