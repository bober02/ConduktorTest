package conduktor.exercise;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class KafkaSeekingDataProvider implements RecordsDataProvider {


    private static final Logger LOG = LoggerFactory.getLogger(KafkaSeekingDataProvider.class);

    // TODO further caching could be implemented for instance on data aggregation
    private final Map<String, KafkaConsumer<String, String>> cachedConsumers;
    // we could use sth like zk to find it
    private final String brokerAddress;

    public KafkaSeekingDataProvider(String brokerAddress) {
        //to avoid creation of consumers on each call and keep one per topic
        cachedConsumers = new HashMap<>();
        this.brokerAddress = brokerAddress;
    }


    @Override
    public List<String> getRecords(String topic, int startingOffset, int count) {

        KafkaConsumer<String, String> consumer = getorCreateKafkaConsumer(topic);
        Set<TopicPartition> partitions = consumer.assignment();

        LOG.info("Searching for topic {} starting at index {} across {} partitions, returning max {} results from each", topic, startingOffset, partitions.size(), count);

        // set starting offset for each partition
        partitions.forEach(topicPartition -> consumer.seek(topicPartition, startingOffset));
        // get all records from starting offset across all partitions

        // TODO in case we have offset larger the end offset kafka will wait till timeout for any new data that might make the offset valid
        // we could check endoffsets and re-query partitions, but that is an extra call, so not sure if it is worth it
        ConsumerRecords<String, String> allRecords = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
        // combine the results up to N from each partition
        List<String> result = new ArrayList<>();
        partitions.forEach(partition -> {
            List<ConsumerRecord<String, String>> partitionRecords = allRecords.records(partition);
            List<String> limitedRecords = partitionRecords.stream().map(ConsumerRecord::value).limit(count).toList();
            LOG.info("Partition {} -> {} results", partition.partition(), limitedRecords.size());
            result.addAll(limitedRecords);
        });
        LOG.info("Total number of results across {} partitions: {}", partitions.size(), result.size());
        return result;
    }

    @Override
    public void close() {
        cachedConsumers.values().forEach(KafkaConsumer::close);
    }

    private KafkaConsumer<String, String> getorCreateKafkaConsumer(String topic) {
        return cachedConsumers.computeIfAbsent(topic, (t) -> {
            LOG.info("Creating Kafka consumer for topic {}", topic);
            final Properties props = new Properties();
            props.put("bootstrap.servers", brokerAddress);
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
            // Get the partitions for the topic and assign them
            List<TopicPartition> partitions = kafkaConsumer.partitionsFor(topic).stream().map(info -> new TopicPartition(info.topic(), info.partition())).toList();
            LOG.info("Assigning {} partitions", partitions.size());
            kafkaConsumer.assign(partitions);
            return kafkaConsumer;
        });
    }
}
