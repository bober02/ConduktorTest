package conduktor.exercise;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.tools.javac.Main;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaDataWriter {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaDataWriter.class);

    private final String kafkaBrokerAddress;

    public KafkaDataWriter(String kafkaBrokerAddress) {
        this.kafkaBrokerAddress = kafkaBrokerAddress;
    }


    public void loadDataFromFile(String topic, int numPartitions, String filename) {
        createTopicIfNotExists(topic, numPartitions);
        InputStreamReader jsonStream = new InputStreamReader(Main.class.getClassLoader().getResourceAsStream(filename));
        JsonObject jsonData = JsonParser.parseReader(jsonStream).getAsJsonObject();

        JsonArray peopleArray = jsonData.getAsJsonArray("ctRoot");
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokerAddress);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 2. Create the Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        try {
            LOG.info("Storing {} records to Kafka broker {}", peopleArray.size(), kafkaBrokerAddress);
            peopleArray.forEach(element -> {
                JsonObject person = element.getAsJsonObject();
                String id = person.get("_id").getAsString();
                producer.send(new ProducerRecord<>(topic, id, person.toString()));
            });
            LOG.info("Completed storage.");
        } catch (Exception e){
            LOG.error("Error while storing records: ", e);
            throw new RuntimeException(e);
        }
        finally {
            producer.flush();
            producer.close();
        }

    }

    private void createTopicIfNotExists(String topicName, int numPartitions) {
        Properties config = new Properties();
        config.put("bootstrap.servers", kafkaBrokerAddress);

        try (AdminClient adminClient = AdminClient.create(config)) {
            // Check if the topic already exists
            Set<String> existingTopics = adminClient.listTopics().names().get();

            if (existingTopics.contains(topicName)) {
                LOG.info("Topic {} already exists. Skipping creation....", topicName);
            } else {
                LOG.info("Creating topic {} with {} partitions", topicName, numPartitions);
                // Topic doesn't exist, create it
                NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
                // this should be the default but set it anyway
                newTopic.configs(Collections.singletonMap("cleanup.policy", "delete"));
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                LOG.info("Topic creation succeeded.");
            }
        } catch (ExecutionException | InterruptedException e) {
            LOG.error("Error while checking or creating topic: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    // Visible for testing
    void deleteTopic(String topic) {
        Properties config = new Properties();
        config.put("bootstrap.servers", kafkaBrokerAddress);

        try (AdminClient adminClient = AdminClient.create(config)) {
            // Delete the topic
            DeleteTopicsResult result = adminClient.deleteTopics(Collections.singletonList(topic));

            // TODO Kafka despite having all futures acked, it still throws exception if the topic is quickly recreated
            // simple solution is to add a bit of a delay
            result.all().get();
            Thread.sleep(500);
            LOG.info("Successfully deleted topic {}", topic);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.load(Main.class.getClassLoader().getResourceAsStream("config.properties"));
        String topic = props.getProperty("topic");
        String kafkaBrokerAddress = props.getProperty("kafkaBrokerAddress");
        KafkaDataWriter writer = new KafkaDataWriter(kafkaBrokerAddress);
        writer.loadDataFromFile(topic, Integer.parseInt(props.getProperty("partitions")), "random-people-data.json");
    }


}