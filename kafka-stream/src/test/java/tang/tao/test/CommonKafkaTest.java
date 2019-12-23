package tang.tao.test;

import kafka.KafkaTest;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.ObjectUtils;

import java.time.Duration;
import java.util.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class CommonKafkaTest {

    private  final static String  topic = "topic-json-calculate";
    @Test
    public void producer(){
        Map<String,Object> config = new HashMap<String,Object>();
        config.put("bootstrap.servers","127.0.0.1:9092");
        config.put("client.id","kafka-producer-client-id");
        //config.put("transactional.id",topic+"transactional_id");
        config.put("retries",0);
        config.put("connections.max.idle.ms",5*60*1000);
        KafkaProducer producer = new KafkaProducer(config,new JsonSerializer<String>(),new JsonSerializer<String>());
        //producer.beginTransaction();

        while(true){
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ProducerRecord<String, String> record = new ProducerRecord(topic, "{\"name\":\"tangtao\",\"sex\":\"男\"}");
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("生产消息"+metadata.toString());
                    //exception.printStackTrace();
                }
            });
        }

        //producer.commitTransaction();
    }

    @Test
    public void consumer(){
        Map<String,Object> config = new HashMap<String,Object>();
        config.put("bootstrap.servers","127.0.0.1:9092");
        config.put("client.id","kafka-consumer-client-id");
        config.put("retries",0);
        config.put("connections.max.idle.ms",5*60*1000);
        config.put("heartbeat.interval.ms",100);
        config.put("group.id","test-group");
        KafkaConsumer consumer = new KafkaConsumer(config,new JsonDeserializer<String>(),new JsonDeserializer<String>());
        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        consumer.subscribe(topics,new ConsumerRebalanceListener(){

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("撤销topic partition"+partitions.size());
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("分配topic partition"+partitions.size());
            }
        });
        while (true){
            ConsumerRecords records =  consumer.poll(Duration.ofSeconds(2));
            Iterator<ConsumerRecord<?, ?>> iterator = records.iterator();
           while (iterator.hasNext()){
               System.out.println("消费数据"+iterator.next().toString());
           }
        }

    }
}
