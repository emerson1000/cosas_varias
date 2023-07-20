package com.function;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoClient;
import org.bson.Document;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {
    private static final int THREADS = 100;
    private static final String API_KEY = "1QR3UQQ2Z8ZiLOgz";
    private static final String API_SECRET = "bKntAM1fYBudf5dT8HCTNGLQ";
    private static final String API_URL = "https://partner-api.voxy.com/partner_api/partners/users";
    private static final Logger LOGGER = Logger.getLogger(Function.class.getName());

    @FunctionName("processUsers")
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        MongoClient mongoClient = null;
        try {
            mongoClient = MongoClients.create("mongodb://cmcavcdeveastus2:qh8QS96NrpQzsuvn3OW40KSULWcazk9DSFKqY8FO5tkaBYwoAF2X9aS0lqSKIM1hXyxHs7woMmMJACDbx6gIfg==@cmcavcdeveastus2.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@cmcavcdeveastus2@");
            MongoDatabase database = mongoClient.getDatabase("integracionvoxycornerstone-db");
            MongoCollection<Document> collection = database.getCollection("staging3");

            long startTime = System.currentTimeMillis();
            int page = 1;
            OkHttpClient client = new OkHttpClient().newBuilder().build();
            ExecutorService executorService = Executors.newFixedThreadPool(THREADS);

            while (true) {
                Map<String, String> params = new HashMap<>();
                params.put("page", String.valueOf(page));

                String signature = createSignature(params);
                String authHeader = "Voxy " + API_KEY + ":" + signature;

                Request apiRequest = new Request.Builder()
                        .url(API_URL + "?page=" + page)
                        .method("GET", null)
                        .addHeader("Authorization", authHeader)
                        .build();

                try (Response response = client.newCall(apiRequest).execute()) {
                    String responseBody = response.body().string();

                    // Parsea el response body como JSON
                    JSONObject json = new JSONObject(responseBody);

                    // Obtiene el array de users
                    JSONArray users = json.getJSONArray("users");

                    if (users.length() == 0) {
                        break;  // No hay más usuarios, detén el bucle
                    }

                    // Loop entre cada user's external_user_id
                    for (int i = 0; i < users.length(); i++) {
                        JSONObject user = users.getJSONObject(i);
                        if (!user.isNull("external_user_id")) {
                            String external_user_id = user.getString("external_user_id");

                            executorService.submit(() -> {
                                try {
                                    fetchAndPrintActivities(client, external_user_id, collection);
                                } catch (IOException e) {
                                    LOGGER.log(Level.SEVERE, "Error al procesar usuarios", e);
                                }
                            });
                        }
                    }

                    // Checkea si hay en la siguiente pagina
                    if (json.getInt("current_page") == json.getInt("total_pages")) {
                        break;
                    } else {
                        page++;
                    }
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Error al procesar usuarios", e);
                }
            }

            executorService.shutdown();
            try {
                // Espera a que todos los hilos terminen
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long endTime = System.currentTimeMillis();
            long executionTime = endTime - startTime;

            return request.createResponseBuilder(HttpStatus.OK).body("Operación terminada con éxito en: " + formatTime(executionTime)).build();
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
    }

    private static void fetchAndPrintActivities(OkHttpClient client, String external_user_id, MongoCollection<Document> collection) throws IOException {
        String signature = DigestUtils.sha256Hex(API_SECRET);
        String authHeader = "Voxy " + API_KEY + ":" + signature;

        String userActivityUrl = API_URL + "/" + external_user_id + "/time_on_task";
        Request request = new Request.Builder()
                .url(userActivityUrl)
                .method("GET", null)
                .addHeader("Authorization", authHeader)
                .build();

        try (Response response = client.newCall(request).execute()) {
            String responseBody = response.body().string();
            JSONObject activities = new JSONObject(responseBody);

            Document doc = new Document("external_user_id", external_user_id);

            for (String key : activities.keySet()) {
                int time = activities.getInt(key);

                if (time > 0) {
                    doc.append(key, time);
                }
            }

            if (doc.size() > 1) {
                collection.insertOne(doc);
            }
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Error al procesar la actividad del usuario " + external_user_id, ex);
        }
    }

    private static String createSignature(Map<String, String> params) {
        List<String> keys = new ArrayList<>(params.keySet());
        Collections.sort(keys);
        StringBuilder sb = new StringBuilder(API_SECRET);
        for (String key : keys) {
            sb.append(key).append("=").append(params.get(key));
        }
        return DigestUtils.sha256Hex(sb.toString());
    }

    private static String formatTime(long milliseconds) {
        long seconds = TimeUnit.MILLISECONDS.toSeconds(milliseconds) % 60;
        long minutes = TimeUnit.MILLISECONDS.toMinutes(milliseconds) % 60;
        long hours = TimeUnit.MILLISECONDS.toHours(milliseconds);
        return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }
}
