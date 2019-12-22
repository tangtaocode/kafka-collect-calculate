package tang.tao.test;

import kafka.KafkaTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class CommonKafkaTest {
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @Test
    public void producer(){
        kafkaTemplate.se
    }

    @Test
    public void consumer(){

    }
}
