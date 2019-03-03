import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;


public class TestEventHubProducer {

    public static void main(String... args) throws Exception {
        //Create Kafka Producer
        final Producer<Long, String> producer = createProducer();

        final String topic = args[0];
        final String payload = args[1];
        final Integer num_of_thread = Integer.parseInt(args[2]);

        final ExecutorService executorService = Executors.newFixedThreadPool(num_of_thread);

        //Run NUM_THREADS TestDataReporters
        for (int i = 0; i < num_of_thread; i++)
            executorService.execute(new ProducerThread(producer, topic,payload));
    }

    private static Producer<Long, String> createProducer() {
        try{
            Properties properties = new Properties();
             properties.load(  TestEventHubProducer.class.getResourceAsStream("producer.config"));
            properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
          properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000");

            return new KafkaProducer<>(properties);
        } catch (Exception e){
            System.out.println("Failed to create producer with exception: " + e);
            System.exit(0);
            return null;        //unreachable
        }
    }
}


