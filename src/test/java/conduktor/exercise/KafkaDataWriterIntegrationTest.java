package conduktor.exercise;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaDataWriterIntegrationTest {

    private static KafkaContainer kafkaContainer;
    private static String kafkaBrokerAddress;
    private final String topic = "test";
    KafkaDataWriter backFill;

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
    }

    @AfterEach
    public void deleteData() {
        backFill.deleteTopic(topic);
    }

    @Test
    public void testTopicWithPartitionsisCreated() {

        KafkaConsumer<String, String> kafkaConsumer = backfillAndGetConsumer();
        // Get the partitions for the topic and assign them
        List<TopicPartition> partitions = kafkaConsumer.partitionsFor(topic).stream().map(info -> new TopicPartition(info.topic(), info.partition())).toList();

        assertThat(partitions).hasSize(2);
    }

    @Test
    public void testAllDataIsStored() {
        KafkaConsumer<String, String> kafkaConsumer = backfillAndGetConsumer();

        List<TopicPartition> partitions = kafkaConsumer.partitionsFor(topic).stream().map(info -> new TopicPartition(info.topic(), info.partition())).toList();
        // use end offsets
        long totalRecordCount = kafkaConsumer.endOffsets(partitions).values().stream().mapToLong(x -> x).sum();
        assertThat(totalRecordCount).isEqualTo(5);

    }

    @Test
    public void testFullJsonIsStored() {
        KafkaConsumer<String, String> kafkaConsumer = backfillAndGetConsumer();

        List<TopicPartition> partitions = kafkaConsumer.partitionsFor(topic).stream().map(info -> new TopicPartition(info.topic(), info.partition())).toList();
        // use end offsets
        kafkaConsumer.assign(partitions);
        partitions.forEach(p -> kafkaConsumer.seek(p, 0));
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
        assertThat(records.count()).isEqualTo(5);
        Set<String> expectedIds = Set.of("testId1","testId2","testId3", "testId4","testId5");
        records.records(topic).forEach(record -> {
            assertThat(expectedIds).contains(record.key());
        });

    }

    private KafkaConsumer<String, String> backfillAndGetConsumer() {
        backFill.loadDataFromFile(topic, 2, "test-data.json");

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokerAddress);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<>(props);
    }
}