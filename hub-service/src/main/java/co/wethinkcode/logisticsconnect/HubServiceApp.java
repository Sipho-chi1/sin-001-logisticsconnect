package co.wethinkcode.logisticsconnect;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.http.Context;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

public class HubServiceApp {


    private static List<Hub> cachedHubs = List.of();

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7051);

        app.get("/health", ctx -> ctx.result("OK"));

        app.get("/hubs/{hubId}", HubServiceApp::getHubById);


        app.get("/hubs", HubServiceApp::hubs);

        fetchHubsFromIngestion();
    }

    private static void getHubById(Context ctx) {
        String id = ctx.pathParam("hubId");

        for (Hub hub : cachedHubs) {
            if (hub.getHub_id().equals(id)) {
                ctx.json(hub);
                return;
            }
        }

        ctx.status(404).result("Hub not found: " + id);
    }


    private static void fetchHubsFromIngestion() {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:7050/hubs"))
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

    private static void hubs(Context ctx) {
        ctx.json(cachedHubs);
    }
}
