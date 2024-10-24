package conduktor.exercise;

import com.google.gson.*;
import com.sun.tools.javac.Main;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class RestAPIServer {

    private static final Logger LOG = LoggerFactory.getLogger(RestAPIServer.class);

    private final String host;
    private final int port;
    private final RecordsDataProvider dataProvider;
    private final Gson gson;
    private Javalin webServer;


    public RestAPIServer(String hostPortString, RecordsDataProvider dataProvider) {
        String[] hostPort = hostPortString.split(":");
        this.host = hostPort[0];
        this.port = Integer.parseInt(hostPort[1]);
        this.gson = new GsonBuilder().setPrettyPrinting().create();
        this.dataProvider = dataProvider;
    }

    public void start() throws Exception {
        if (webServer != null) return;

        webServer = Javalin.create();
        // Define the route for the GET request with path parameters and query parameter
        webServer.get("/topic/{topic_name}/{offset}", ctx -> {
            String topicName = ctx.pathParam("topic_name");  // Get topic_name from path
            int offset = Integer.parseInt(ctx.pathParam("offset"));  // Get offset from path
            int count = ctx.queryParamAsClass("count", Integer.class).getOrDefault(10);  // Get count (default = 10)

            LOG.info("GET REQUEST - topic={}, offset={}, count={}", topicName, offset, count);

            List<String> records = dataProvider.getRecords(topicName, offset, count);
            generateAndSendResponse(records, ctx);
        });
        webServer.get("/topic/{topic_name}", ctx -> {
            String topicName = ctx.pathParam("topic_name");  // Get topic_name from path
            int offset = 0;  // Get offset from path
            int count = ctx.queryParamAsClass("count", Integer.class).getOrDefault(10);  // Get count (default = 10)

            LOG.info("GET REQUEST - topic={}, offset={}, count={}", topicName, offset, count);

            List<String> records = dataProvider.getRecords(topicName, offset, count);
            generateAndSendResponse(records, ctx);
        });
        webServer.start(host, port);
        webServer.jettyServer().server().join();
    }

    public void generateAndSendResponse(List<String> records, Context ctx){

        JsonObject rootObject = new JsonObject();

        // Create a JsonArray to hold the list of records
        JsonArray recordsArray = new JsonArray();

        // Parse each string and add it to the JsonArray
        for (String jsonString : records) {
            JsonElement jsonElement = JsonParser.parseString(jsonString);
            recordsArray.add(jsonElement);
        }

        // Attach the records array to the root object
        rootObject.add("records", recordsArray);
        ctx.result(gson.toJson(rootObject));
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.load(Main.class.getClassLoader().getResourceAsStream("config.properties"));
        KafkaSeekingDataProvider dataProvider = new KafkaSeekingDataProvider(props.getProperty("kafkaBrokerAddress"));
        new RestAPIServer(props.getProperty("webServerAddress"), dataProvider).start();
    }
}

