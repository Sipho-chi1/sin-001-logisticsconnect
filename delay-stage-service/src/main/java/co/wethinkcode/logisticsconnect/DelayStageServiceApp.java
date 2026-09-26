package co.wethinkcode.logisticsconnect;

import co.wethinkcode.logisticsconnect.mq.MqConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.http.Context;

import javax.jms.JMSException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.*;

public class DelayStageServiceApp {

    private static List<Hub> cachedHubs = List.of();

    private static final Map<String,Object> stages = new HashMap<>();

    private static DelayEventProducer delay = new DelayEventProducer();

    public static void main(String[] args) throws JMSException {


        Javalin app = Javalin.create().start(7052);

        app.get("/health", ctx -> ctx.result("OK"));

        app.post("/delay-stage/{hubId}",DelayStageServiceApp::insertStage);

        app.get("/delay-stage/{hubId}", DelayStageServiceApp::getStage);


        delay.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                delay.close();
            } catch (JMSException e) {
                System.err.println("Failed to close MQ connection: " + e.getMessage());
            }
        }));



        fetchHubsFromIngestion();

        // TODO (Tracks the Transit Delay Stage (0-8, e.g. weather shutdowns).)
        // Add domain endpoints for delay-stage-service here.

    }

    private static void getStage(Context ctx) {
        String id = ctx.pathParam("hubId");

        if (!stages.containsKey(id)) {
            ctx.status(404).result("No stage recorded for hub: " + id);
            return;
        }

        Map<String, Object> response = new HashMap<>();
        response.put("hubId", id);
        response.put("stage", stages.get(id));

        ctx.json(response);
    }




    private static void fetchHubsFromIngestion() {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:7051/hubs"))
                .header("Accept", "application/json")
                .build();

        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(HttpResponse::body)
                .thenAccept(jsonString -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        cachedHubs = List.of(mapper.readValue(jsonString, Hub[].class));
                    } catch (Exception e) {
                        System.err.println("Failed to parse hubs JSON: " + e.getMessage());
                    }
                })
                .join();
    }

    private static void insertStage(Context ctx)throws JMSException,JsonProcessingException {
        String id = ctx.pathParam("hubId");
        Map<String,Integer> body = ctx.bodyAsClass(Map.class);
        int stage = body.get("stage");
        if (stage < 0 || stage > 8) {
            ctx.status(400).result("Invalid stage: must be between 0 and 8");
            return;
        }

        boolean found = false;
        for (int i = 0; i < cachedHubs.size(); i++) {
            if (cachedHubs.get(i).getHub_id().equals(id)) {
                found = true;
                break;
            }
        }

        if (found==false) {
            ctx.status(404).result("Hub not found: " + id);
            return;
        }
        stages.put(id, stage);

        Map<String, Object> payload = new HashMap<>();
        payload.put("hubId", id);
        payload.put("stage", stage);
        payload.put("timestamp", LocalDateTime.now().toString());

        ObjectMapper mapper = new ObjectMapper();
        String messagePayload = mapper.writeValueAsString(payload);
        delay.sendDelayEvent(messagePayload);

        ctx.status(200).result("Stage updated");
    }


}



class DelayEventProducer {

    private final String brokerUrl = MqConfig.BROKER_URL;
    private final String topicName = MqConfig.TOPIC;

    private Connection connection;
    private Session session;
    private MessageProducer producer;
    private Topic topic;

    public DelayEventProducer() {
    }

    public void start() throws JMSException {
        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        connection = factory.createConnection();
        connection.start();

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        topic = session.createTopic(topicName);
        producer = session.createProducer(topic);
    }

    public void sendDelayEvent(String messagePayload) throws JMSException {
        TextMessage message = session.createTextMessage(messagePayload);
        producer.send(message);
    }

    public void close() throws JMSException {
        if (producer != null) producer.close();
        if (session != null) session.close();
        if (connection != null) connection.close();
    }
}

