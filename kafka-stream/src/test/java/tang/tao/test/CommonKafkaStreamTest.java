package tang.tao.test;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.ObjectUtils;
import scala.reflect.io.Streamable;
import tang.tao.KafkaApplication;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@SpringBootTest(classes = KafkaApplication.class, webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@RunWith(SpringRunner.class)
public class CommonKafkaStreamTest {

    private static final String INPUT_TOPIC="my-input-topic";

    private static final String OUT_TOPIC="my-output-topic";

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
                ProducerRecord<String, String> record = new ProducerRecord(INPUT_TOPIC, i,INPUT_TOPIC+i,"{\"name\":\"tangtao\",\"sex\":\"男\"}");

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(!ObjectUtils.isEmpty(exception)){
                            System.out.println("FAIL生产消息失败");
                            exception.printStackTrace();
                        }
                        System.out.println("生产消息"+metadata.toString());
                    }
                });
            }
        }
    }


    @Test
    public void streamTest() throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(INPUT_TOPIC)
                .mapValues(value -> value)
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5).toMillis()).advanceBy(Duration.ofSeconds(1).toMillis()))
                .aggregate(String::new,(k,v,m)->{m+=v;return m;},
                        Materialized.<String,String,WindowStore<Bytes, byte[]>>as("storemap"))
                .toStream()
                .peek((k,v)->{
                    System.out.println("KEY"+k+":VALUE"+v.toString());
                });
                //.to(OUT_TOPIC, Produced.with(Serdes.String(),Serdes.String()));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Thread.sleep(200000);
    }

}
