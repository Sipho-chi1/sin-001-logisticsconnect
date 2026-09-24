package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;
import io.javalin.http.Context;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class IngestionServiceApp {

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7050);

        app.get("/health", ctx -> ctx.result("OK"));

        // TODO: read and clean src/main/resources/hubs-global.csv (hubs, sorting centers, regional districts data —
        // trim whitespace, fix casing, normalize dates/booleans) and expose the
        // cleaned records here for the other services to consume.
        app.get("/hubs",IngestionServiceApp::handleHubs);
    }

    private static void handleHubs(Context ctx) {
        try {
            List<String> lines = Files.readAllLines(
                    Path.of("ingestion-service/src/main/resources/hubs-global.csv")
            );

            List<Hub> cleanedLines = Hub.parseAndCleanRow(lines);
            ctx.json(cleanedLines);
        } catch (Exception e) {
            ctx.status(500).result("Failed to read hubs CSV: " + e.getMessage());
        }
    }
}
