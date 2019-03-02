import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestEventHubConsumer {
    private final static int NUM_THREADS = 10;

    public static void main(String... args) throws Exception {
        final String topic= args[0];
        final Integer number_Of_thread=Integer.parseInt(args[1]);
        final ExecutorService executorService = Executors.newFixedThreadPool(number_Of_thread);

        for (int i = 0; i < number_Of_thread; i++){
            executorService.execute(new ConsumerThread(topic));
        }
    }
}
