package com.function;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import org.bson.Document;
import org.bson.conversions.Bson;
import java.util.concurrent.ThreadLocalRandom;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class Function {
    private static final String API_URL = "https://partner-api.voxy.com/partner_api/partners/user_time_on_task";
    private static final int THREADS = 100;
    private static final int MAX_RETRIES = 5;  
    private static final Logger LOGGER = Logger.getLogger(Function.class.getName());

    private String apiSecret;
    private String apiKey;  

    @FunctionName("timeontask")
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        MongoClient mongoClient = null;
        
        SecretClient secretClient = new SecretClientBuilder()
        .vaultUrl("https://kv-ca-vc-dev-east2.vault.azure.net/")
        .credential(new DefaultAzureCredentialBuilder().build())
        .buildClient();
         
        String dbName = secretClient.getSecret("dbname").getValue();
        String connectionString = secretClient.getSecret("connectionstring").getValue();
        this.apiSecret = secretClient.getSecret("apisecret").getValue();
        this.apiKey = secretClient.getSecret("apikey").getValue();
        
        try {
            mongoClient = MongoClients.create(connectionString);
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection("timeontask10");

            long startTime = System.currentTimeMillis();
            int page = 1;

            OkHttpClient client = new OkHttpClient().newBuilder().build();
            ExecutorService executorService = Executors.newFixedThreadPool(THREADS);

            while (true) {
                Map<String, String> params = new HashMap<>();
                params.put("has_entries", "true");
                params.put("page", String.valueOf(page));
                
                //crear firma
                String signature = createSignature(params, this.apiSecret);
                System.out.println("Firma: " + signature); // imprime la firma
                String authHeader = "Voxy " + apiKey + ":" + signature;

                //// Ordenar las llaves de los parámetros
                List<String> keys = new ArrayList<>(params.keySet());
                Collections.sort(keys);

                 // Construir la URL con parámetros
            HttpUrl.Builder urlBuilder = HttpUrl.parse(API_URL).newBuilder();
            for (String key : keys) {
                urlBuilder.addQueryParameter(key, params.get(key));
            }
            HttpUrl url = urlBuilder.build();
                // Imprime los parámetros
              for (Map.Entry<String,String> entry : params.entrySet()) {
              System.out.println("Clave: " + entry.getKey() + ", Valor: " + entry.getValue());
               }

               Request apiRequest = new Request.Builder()
                .url(url)
                .method("GET", null)
                .addHeader("AUTHORIZATION", authHeader)
                .build();

                try (Response response = client.newCall(apiRequest).execute()) {
                    String responseBody = response.body().string();
                    System.out.println("Respuesta de la API: " + response.code() + " " + response.message());    

                    if (responseBody == null || responseBody.isEmpty()) {
                      LOGGER.log(Level.SEVERE, "API response is empty or null.");
                      return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("API response is empty or null.").build();                                
                    }

                    JSONObject json;
                    try {
                        json = new JSONObject(responseBody);
                    } catch (JSONException e) {
                        LOGGER.log(Level.SEVERE, "Unable to parse API response into JSON object.", e);
                        return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Unable to parse API response into JSON object.").build();
                    }
    
                    if (!json.has("results")) {
                        context.getLogger().log(Level.SEVERE, "API response does not contain 'results'.");
                        return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("API response does not contain 'results'.").build();
                    }

                    JSONArray results = json.getJSONArray("results");

                    if (results.length() == 0) {
                        break;  // No hay más usuarios, detén el bucle
                    }

                    for (int i = 0; i < results.length(); i++) {
                        JSONObject user = results.getJSONObject(i);
                 
               
            
                        executorService.submit(() -> {
                            try {
                                fetchAndPrintActivities(user, collection,  this.apiSecret, this.apiKey);
                            } catch (Exception e) {
                                LOGGER.log(Level.SEVERE, "Error al procesar usuarios", e);
                            }
                        });
                    }
                    
                    // Verifica si el campo 'next' está presente y si es nulo
                    if (!json.has("next") || json.isNull("next")) {
                        break;  // No hay más páginas, detén el bucle
                    }

                    page++;
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Error al procesar usuarios", e);
                }
            }

            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(24, TimeUnit.HOURS)) {
                    executorService.shutdownNow();
                    if (!executorService.awaitTermination(60, TimeUnit.SECONDS))
                        System.err.println("Pool de hilos no terminó");
                }
            } catch (InterruptedException ie) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
            
            long endTime = System.currentTimeMillis();
            long executionTime = endTime - startTime;

            return request.createResponseBuilder(HttpStatus.OK).body("Operación terminada con éxito en: " + formatTime(executionTime)).build();
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
                   // Añade el tiempo de suspensión aquí...
    try {
        Thread.sleep(400);  // Sleep for 400 milliseconds
    } catch (InterruptedException e) {
        LOGGER.log(Level.SEVERE, "Error durante el tiempo de espera", e);
    }
  }         
}      
}   

    private static void fetchAndPrintActivities(JSONObject user, MongoCollection<Document> collection, String apiSecret, String apiKey) throws IOException {
        Document doc = new Document();
        Map<String, String> activityNames = new HashMap<>();
       activityNames.put("lessons", "Tiempo en Lecciones");
       activityNames.put("private_sessions", "Tiempo en Clases Privadas");
       activityNames.put("group_sessions", "Tiempo en Clases Grupales");
       activityNames.put("assessments", "Tiempo en VPA");
       activityNames.put("word_bank", "Tiempo en Word Bank");  // incluyendo 'word_bank' en las actividades
       activityNames.put("grammar_guide", "Tiempo en Grammar Guide");  // incluyendo 'grammar_guide' en las actividades
        
       for (String key : user.keySet()) {
        if (!activityNames.containsKey(key)) {
            // Si la clave no es una actividad, simplemente la agregamos al documento
            Object value;
            if(key.equals("external_user_id")){
                if (!user.isNull(key)) { // verifica si el external_user_id no es nulo    
                value=user.getString(key);
        }else{
               value = null;
        }
     } else {
           value=user.get(key);
      }  
        
            doc.append(key, value);    
        }else{    // Si la clave es una actividad, verificamos el tiempo
             int time = user.getInt(key);
             if (time > 60) {
                 String formattedTime = formatTime(time);
                 doc.append(activityNames.get(key), formattedTime);
             }
         }
     }
    
      // Verificar si total_time_on_task es mayor a 60
    int total_time_on_task = user.getInt("total_time_on_task");
    if (total_time_on_task > 60) {
        String formattedTime = formatTime(total_time_on_task);
        doc.append("total_time_on_task", formattedTime);

      // Solo insertamos el documento si tiene al menos una actividad con tiempo mayor a 60
    
      doDatabaseOperationWithRetry(collection, doc, 0);
    }
    }
  
   
    private static String formatTime(int seconds) {
        if (seconds < 3600) {
            // Menos de 1 hora
            return String.format("%d min", (int)Math.round(seconds / 60.0));
        }
        else if (seconds < 86400) {
            // Menos de 1 día
            int minutes = (seconds % 3600) / 60;
            int hours = seconds / 3600;
            return String.format("%d hr %d min", hours, minutes);
        }
        else {
            // Más de 1 día
            int minutes = (seconds % 3600) / 60;
            int hours = (seconds % 86400) / 3600;
            int days = seconds / 86400;
            return String.format("%d:%02d:%02d", days, hours, minutes);
        }
    }

    private static String createSignature(Map<String, String> params,String apiSecret) {
        //ordena las llaves
        List<String> keys = new ArrayList<>(params.keySet());
        Collections.sort(keys);
        //crea el parametro string con orden alfabetico
        StringBuilder sb = new StringBuilder();
        for (String key : keys) {
            sb.append(key).append("=").append(params.get(key)).append("&");
        }
        // Delete the last '&'
    if (sb.length() > 0) {
        sb.deleteCharAt(sb.length() - 1);
    }
    // Create the signature
    String signature = DigestUtils.sha256Hex(apiSecret + sb.toString());

    return signature;
}

    private static String formatTime(long milliseconds) {
        long seconds = TimeUnit.MILLISECONDS.toSeconds(milliseconds) % 60;
    long minutes = TimeUnit.MILLISECONDS.toMinutes(milliseconds) % 60;
    long hours = TimeUnit.MILLISECONDS.toHours(milliseconds);
    return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }
    private static void doDatabaseOperationWithRetry(MongoCollection<Document> collection, Document doc, int retryCount) {
    try {
        // Supongamos que "external_user_id" es único para cada usuario
        String externalUserId = doc.getString("external_user_id");
        LOGGER.log(Level.INFO, "Procesando usuario con external_user_id: " + externalUserId);

        Bson filter = Filters.eq("external_user_id", externalUserId);
        FindIterable<Document> result = collection.find(filter);
          // Si el usuario no existe en la base de datos, insertamos el nuevo documento
        if (!result.iterator().hasNext()) {
            LOGGER.log(Level.INFO, "Insertando nuevo usuario con external_user_id: " + externalUserId);
            collection.insertOne(doc);
        } else {
            // Si el usuario ya existe, reemplazamos el documento existente con el nuevo
            LOGGER.log(Level.INFO, "Actualizando usuario existente con external_user_id: " + externalUserId);
            collection.replaceOne(filter, doc);
        }
        
    } catch (MongoCommandException e) {
        if (e.getErrorCode() == 16500 && retryCount < MAX_RETRIES) {  // El código 16500 indica una tasa de solicitudes demasiado alta
            try {
                long waitTime = (long) Math.pow(2, retryCount) * 1000;
                long jitter = ThreadLocalRandom.current().nextLong(waitTime / 2);
                waitTime += jitter;
                Thread.sleep(waitTime);  // Espera un tiempo exponencial más un jitter antes de reintentar
            } catch (InterruptedException ie) {
                LOGGER.log(Level.SEVERE, "Error durante el tiempo de espera", ie);
                Thread.currentThread().interrupt();
            }
            doDatabaseOperationWithRetry(collection, doc, retryCount + 1);
        } else {
            throw e;
        }
    }
}
    
    }

