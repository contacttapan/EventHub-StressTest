import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerThread implements Runnable {

    private final String TOPIC;
    private final ObjectMapper mapper=new ObjectMapper();

    
    //Each consumer needs a unique client ID per thread
    private static int id = 0;

    public ConsumerThread(final String TOPIC){
        this.TOPIC = TOPIC;
    }

    public void run (){
        final Consumer<Long, String> consumer = createConsumer();
        System.out.println("Polling");

        try {
            while (true) {
                final ConsumerRecords<Long, String> consumerRecords = consumer.poll(10);
                for(ConsumerRecord<Long, String> cr : consumerRecords) {
                  //LogisticUnit outputObject = mapper.readValue( cr.value(), LogisticUnit.class);
                  System.out.printf("Consumer Record:(%d, %d, %d)\n", cr.key(), cr.partition(), cr.offset());
                }
                consumer.commitAsync();
            }
        } catch (CommitFailedException e) {
            System.out.println("CommitFailedException: " + e);
        }finally{
            consumer.close();
        }
    }

    private Consumer<Long, String> createConsumer() {
        try {
            final Properties properties = new Properties();
            synchronized (ConsumerThread.class) {
                properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaExampleConsumer#" + id);
                id++;
            }
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.load(  TestEventHubConsumer.class.getResourceAsStream("consumer.config"));
            // Create the consumer using properties.
            final Consumer<Long, String> consumer = new KafkaConsumer<>(properties);

            //consume from begining
            consumer.seekToBeginning(consumer.assignment());

            // Subscribe to the topic.
            consumer.subscribe(Collections.singletonList(TOPIC));
            return consumer;
            
        } catch (FileNotFoundException e){
            System.out.println("FileNoteFoundException: " + e);
            System.exit(1);
            return null;        //unreachable
        } catch (IOException e){
            System.out.println("IOException: " + e);
            //System.exit(1);
            return null;        //unreachable
        }
    }
}
