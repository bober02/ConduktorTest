package conduktor.exercise;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaSeekingDataProviderTest {

    private static KafkaContainer kafkaContainer;
    private static String kafkaBrokerAddress;
    private final String topic = "test";
    private final String testFile = "test-data.json";
    KafkaDataWriter backFill;
    KafkaSeekingDataProvider dataProvider;

    @BeforeAll
    public static void setUp() {
        // Start Kafka container
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));
        kafkaContainer.start();
        kafkaBrokerAddress = kafkaContainer.getBootstrapServers().split("PLAINTEXT://")[1];
    }

    @AfterAll
    public static void tearDown() {
        // Stop Kafka container
        if (kafkaContainer != null) {
            kafkaContainer.stop();
        }
    }

    @BeforeEach
    public void setUpTest() {
        backFill = new KafkaDataWriter(kafkaBrokerAddress);
        dataProvider = new KafkaSeekingDataProvider(kafkaBrokerAddress);
    }

    @AfterEach
    public void deleteData() {
        backFill.deleteTopic(topic);
    }

    @Test
    public void testNoData() {
        List<String> records = dataProvider.getRecords(topic, 0, 10);
        assertThat(records).isEmpty();
    }

    @Test
    public void testAllDataReturned() {
        backFill.loadDataFromFile(topic, 2, testFile);
        List<String> records = dataProvider.getRecords(topic, 0, 10);
        assertThat(records).hasSize(5);
    }

    @Test
    public void testSubsetOfDataReturned(){
        backFill.loadDataFromFile(topic, 2, testFile);
        // we want 1 record from each partition
        List<String> records = dataProvider.getRecords(topic, 0, 1);
        assertThat(records).hasSize(2);
    }

    @Test
    public void testLargeNumberofPartitions(){
        backFill.loadDataFromFile(topic, 20, testFile);
        // we want 1 record from each partition
        List<String> records = dataProvider.getRecords(topic, 0, 10);
        assertThat(records).hasSize(5);
    }

    @Test
    public void testNonZeroindex(){
        backFill.loadDataFromFile(topic, 2, testFile);
        // we remove the first element from 2 partitions
        List<String> records = dataProvider.getRecords(topic, 1, 5);
        assertThat(records).hasSize(3);
    }

    @Test
    public void testIndexBeyondEndOfPartitions(){
        backFill.loadDataFromFile(topic, 2, testFile);
        // we remove the first element from 2 partitions
        List<String> records = dataProvider.getRecords(topic, 100, 5);
        assertThat(records).isEmpty();
    }



}
