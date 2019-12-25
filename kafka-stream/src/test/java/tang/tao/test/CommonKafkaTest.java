package tang.tao.test;

import kafka.KafkaTest;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.ObjectUtils;

import javax.swing.plaf.synth.SynthTextAreaUI;
import java.time.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(SpringRunner.class)
@SpringBootTest
public class CommonKafkaTest {

    private  final static String  topic = "topic-json-stream";
    @Test
    public void changeTopic(){
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,30000);
        AdminClient client = AdminClient.create(props);
        client.deleteTopics(Arrays.asList(topic));
        client.close();

    }

    @Test
    public void producer(){
        Map<String,Object> config = new HashMap<String,Object>();
        config.put("bootstrap.servers","127.0.0.1:9092");
        config.put("client.id","kafka-producer-client-id");
        config.put("acks","1");
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
            for(int i=0;i<3;i++){
                ProducerRecord<String, String> record = new ProducerRecord(topic, i,topic+i,"{\"name\":\"tangtao\",\"sex\":\"男\"}");

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(!ObjectUtils.isEmpty(exception)){
                            exception.printStackTrace();
                        }
                        System.out.println("生产消息"+metadata.toString());
                    }
                });
            }
        }

        //producer.commitTransaction();
    }

    @Test
    public void consumer() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                runConsumer("one");
            }
        });
        executor.execute(new Runnable() {
            @Override
            public void run() {
                runConsumer("two");
            }
        });
        executor.execute(new Runnable() {
            @Override
            public void run() {
                runConsumer("three");
            }
        });
        Thread.sleep(60000000);
    }

    private void runConsumer(String number){
        Map<String,Object> config = new HashMap<String,Object>();
        config.put("bootstrap.servers","127.0.0.1:9092");
        config.put("client.id","kafka-consumer-client-id"+number);
        config.put("connections.max.idle.ms",5*60*1000);
        config.put("heartbeat.interval.ms",100);
        //latest:earliest
        config.put("auto.offset.reset","earliest");
        config.put("enable.auto.commit","false");
        config.put("group.id","test-group");

        KafkaConsumer consumer = new KafkaConsumer(config,new StringDeserializer(),new StringDeserializer());
        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        consumer.subscribe(topics,new ConsumerRebalanceListener(){

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                //可手工空置消费partitions自平衡消费
                System.out.println(number+"撤销topic partition"+partitions.size());
            }
            //可手工移除或空置消费partitions自平衡消费
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println(number+"分配topic partition"+partitions.size());
            }
        });
        while (true){
            ConsumerRecords records =  consumer.poll(Duration.ofSeconds(2));
            Iterator<ConsumerRecord<?, ?>> iterator = records.iterator();
           while (iterator.hasNext()){
               ConsumerRecord ccord = iterator.next();

               Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>(1);
               TopicPartition topicPartition = new TopicPartition(ccord.topic(),ccord.partition());
               OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(ccord.offset()+1,LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli()+"");
               offsets.put(topicPartition,offsetAndMetadata);
               consumer.commitSync(offsets);

               Date time = new Date(ccord.timestamp());
               System.out.println(number+"消费数据日期"+time.toString());
               System.out.println(number+"消费数据"+ccord.toString());
               System.out.println(number+"提交消费偏移量"+ccord.offset());
           }

        }

    }
}
