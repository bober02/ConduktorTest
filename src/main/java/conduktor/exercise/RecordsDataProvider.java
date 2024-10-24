package conduktor.exercise;

import java.util.List;

public interface RecordsDataProvider {
    /**
     * Fetches a list of records based on the topic, offset, and count.
     *
     * @param topic The name of the topic or data source.
     * @param offset The starting point to fetch records.
     * @param count The number of records to fetch.
     * @return A list of records as strings.
     */
    List<String> getRecords(String topic, int offset, int count);

    void close();
}