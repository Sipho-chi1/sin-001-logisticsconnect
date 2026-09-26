package co.wethinkcode.logisticsconnect;

import co.wethinkcode.logisticsconnect.mq.MqConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class TransitServiceApp {

    private static final HttpClient client = HttpClient.newHttpClient();
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Map<String, Integer> latestStages = new HashMap<>();

    private static TransitServiceConsumer TransitConsumer = new TransitServiceConsumer();

    public static void main(String[] args) throws JMSException {
        Javalin app = Javalin.create().start(7053);

        app.get("/health", ctx -> ctx.result("OK"));
        app.get("/eta/{hubId}", TransitServiceApp::calculateEta);

        TransitConsumer.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                TransitConsumer.close();
            } catch (JMSException e) {
                System.err.println("Failed to close MQ connection: " + e.getMessage());
            }
        }));

    }

    public static void updateStage(String hubId, int stage) {
        latestStages.put(hubId, stage);
    }

    private static void calculateEta(Context ctx) {
        String hubId = ctx.pathParam("hubId");

        HttpResponse<String> hubResponse;
        try {
            hubResponse = client.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:7051/hubs/" + hubId))
                            .header("Accept", "application/json")
                            .build(),
                    HttpResponse.BodyHandlers.ofString()
            );
        } catch (Exception e) {
            ctx.status(500).result("Failed to reach hub-service: " + e.getMessage());
            return;
        }

        if (hubResponse.statusCode() == 404) {
            ctx.status(404).result("Hub not found: " + hubId);
            return;
        }

        if (!latestStages.containsKey(hubId)) {
            ctx.status(404).result("No delay stage recorded for hub: " + hubId);
            return;
        }

        try {
            Hub hub = mapper.readValue(hubResponse.body(), Hub.class);
            int stage = latestStages.get(hubId);

            int baseMinutes = 60;
            int etaMinutes = baseMinutes + (stage * 15);

            Map<String, Object> result = Map.of(
                    "hubId", hub.getHub_id(),
                    "province", hub.getProvince(),
                    "sortingCenter", hub.getSorting_center(),
                    "delayStage", stage,
                    "etaMinutes", etaMinutes
            );

            ctx.json(result);
        } catch (Exception e) {
            ctx.status(500).result("Failed to calculate ETA: " + e.getMessage());
        }
    }
}

// MQ TODO: subscribes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.logisticsconnect.mq.MqConfig)

class TransitServiceConsumer {

    private final String brokerUrl = MqConfig.BROKER_URL;
    private final String topicName = MqConfig.TOPIC;

    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private Topic topic;

    public TransitServiceConsumer() {
    }

    public void start() throws JMSException {
        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        connection = factory.createConnection();
        connection.start();

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        topic = session.createTopic(topicName);
        consumer = session.createConsumer(topic);

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    if (message instanceof TextMessage textMessage) {
                        String json = textMessage.getText();
                        ObjectMapper mapper = new ObjectMapper();
                        Map<String, Object> data = mapper.readValue(json, Map.class);

                        String hubId = (String) data.get("hubId");
                        int stage = (int) data.get("stage");

                        TransitServiceApp.updateStage(hubId, stage);
                    }
                } catch (Exception e) {
                    System.err.println("Failed to process delay event: " + e.getMessage());
                }
            }
        });
    }

    public void close() throws JMSException {
        if (consumer != null) consumer.close();
        if (session != null) session.close();
        if (connection != null) connection.close();
    }
}