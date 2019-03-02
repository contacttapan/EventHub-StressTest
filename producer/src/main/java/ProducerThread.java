import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class ProducerThread implements Runnable {

  private static final int NUM_MESSAGES = 1;
  private final String TOPIC;

  private Producer<Long, String> producer;
  private final String payload;

  public ProducerThread(final Producer<Long, String> producer, String TOPIC, String payload) {
    this.producer = producer;
    this.TOPIC = TOPIC;
    this.payload = payload;
  }

  @Override
  public void run() {
    try {
      String data = readFromFile(payload);
      while (true){
        for (int i = 0; i < NUM_MESSAGES; i++) {
          long time = System.currentTimeMillis();
          System.out.println("Test Data #" + i + " from thread #" + Thread.currentThread().getId());
          final ProducerRecord<Long, String> record;
          record = new ProducerRecord<Long, String>(TOPIC, time, data);
          producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
              if (exception != null) {
                System.out.println(exception);
                //System.exit(1);
              }
            }
          });

        }
        System.out.println(
            "Finished sending " + NUM_MESSAGES + " messages from thread #" + Thread.currentThread()
                .getId() + "!");
    }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public String readFromFile(String filename)
      throws IOException {
    InputStream is = getClass().getResourceAsStream(filename);
    InputStreamReader isr = new InputStreamReader(is);
    BufferedReader br = new BufferedReader(isr);
    StringBuffer sb = new StringBuffer();
    String line;
    while ((line = br.readLine()) != null) {
      sb.append(line);
    }
    br.close();
    isr.close();
    is.close();
    return sb.toString();
  }
}