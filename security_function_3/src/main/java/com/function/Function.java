package com.function;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.microsoft.azure.functions.annotation.*;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONException;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import com.microsoft.azure.functions.HttpStatus;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.*;


/**
 * Azure Functions with HTTP Trigger.
 */
public class Function { private static final int THREADS = 100;
    private static final String API_KEY = "1QR3UQQ2Z8ZiLOgz";
    private static final String API_SECRET = "bKntAM1fYBudf5dT8HCTNGLQ";
    private static final String API_URL = "https://partner-api.voxy.com/partner_api/partners/users";
    private static AtomicInteger cuenta_unidades_completadas = new AtomicInteger(0);
    private static AtomicInteger cuenta_unidades_totales = new AtomicInteger(0);
    private static AtomicInteger cuenta_lecciones_completadas = new AtomicInteger(0);
    private static AtomicInteger cuenta_lecciones_totales = new AtomicInteger(0);
    private static final Logger LOGGER = Logger.getLogger(Function.class.getName());
    interface OnUserCompleteCallback {
        void onComplete(String external_user_id);
    }

    @FunctionName("processUsers")
    public HttpResponseMessage run(
        @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
    final ExecutionContext context
) { 
    MongoClient mongoClient = null;
    try {
    mongoClient = MongoClients.create("mongodb://cmcavcdeveastus2:qh8QS96NrpQzsuvn3OW40KSULWcazk9DSFKqY8FO5tkaBYwoAF2X9aS0lqSKIM1hXyxHs7woMmMJACDbx6gIfg==@cmcavcdeveastus2.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@cmcavcdeveastus2@");
    MongoDatabase database = mongoClient.getDatabase("integracionvoxycornerstone-db");
    MongoCollection<Document> collection = database.getCollection("staging2");
    
    long startTime = System.currentTimeMillis();
    

     FindIterable<Document> documents = collection.find();
        for (Document doc : documents) {
            cuenta_unidades_totales.incrementAndGet();
            if (doc.getString("status").equals("complete")) {
                cuenta_unidades_completadas.incrementAndGet();
            }

            Document lessons = (Document) doc.get("lessons");
            for (String key : lessons.keySet()) {
                Document lesson = (Document) lessons.get(key);
                cuenta_lecciones_totales.incrementAndGet();
                if (lesson.getString("status").equals("complete")) {
                    cuenta_lecciones_completadas.incrementAndGet();
                }
            }
        }

    int page = 1;
    OkHttpClient client = new OkHttpClient().newBuilder()
                // .callTimeout(60, TimeUnit.MINUTES)
                .build();
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
    Response response = client.newCall(apiRequest).execute();
    String responseBody = response.body().string();
    Request requestToApi = new Request.Builder()
   .url(API_URL)
   .build();

 // Parsea el response body como JSON
    JSONObject json = new JSONObject(responseBody);
            
    // Obtiene el array de users
     JSONArray users = json.getJSONArray("users");
    if (users.length() == 0) {
    break;  // No hay más usuarios, detén el bucle
    }
    OnUserCompleteCallback callback = external_user_id -> System.out.println("El usuario " + external_user_id + " ha completado todas sus unidades y lecciones");
    // Loop entre cada user's external_user_id
        for (int i = 0; i < users.length(); i++) {
        JSONObject user = users.getJSONObject(i);
        if (!user.isNull("external_user_id")) {
            String external_user_id = user.getString("external_user_id");


        executorService.submit(() -> {
            try {
                fetchAndPrintUserUnits(client, external_user_id, user, collection/*callback*/);
            }catch (IOException e) {
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
        System.out.println("Numero unidades completadas: "+ cuenta_unidades_completadas);
        System.out.println("Numero unidades totales: "+cuenta_unidades_totales);
        if (cuenta_unidades_totales.get()> 0) {
        System.out.println("proporcion numero unidades completadas/totales: " + (cuenta_unidades_completadas.get() / (double) cuenta_unidades_totales.get()));
        } else {
            System.out.println("No hay unidades totales. La proporción no se puede calcular");
        }
        System.out.println("Numero lecciones completadas: "+ cuenta_lecciones_completadas);
        System.out.println("Numero lecciones totales: "+cuenta_lecciones_totales);
        if (cuenta_lecciones_totales.get() > 0) {
        System.out.println("proporcion numero lecciones completadas/totales: " + (cuenta_lecciones_completadas.get() / (double) cuenta_lecciones_totales.get()));
        System.out.println("Tiempo de ejecución: " + formatTime(executionTime));
        } else {
            System.out.println("No hay lecciones totales. La proporción no se puede calcular");
            
        }
        // Inserta las salidas esperadas
Document doc = new Document()
.append("Numero unidades completadas", cuenta_unidades_completadas.get())
.append("Numero unidades totales", cuenta_unidades_totales.get())
.append("Numero lecciones completadas", cuenta_lecciones_completadas.get())
.append("Numero lecciones totales", cuenta_lecciones_totales.get())
.append("Tiempo de ejecución", formatTime(executionTime));

if (cuenta_unidades_totales.get() > 0) {
doc.append("proporcion numero unidades completadas/totales", cuenta_unidades_completadas.get() / (double) cuenta_unidades_totales.get());
} else {
doc.append("proporcion numero unidades completadas/totales", "No hay unidades totales. La proporción no se puede calcular");
}

if (cuenta_lecciones_totales.get() > 0) {
doc.append("proporcion numero lecciones completadas/totales", cuenta_lecciones_completadas.get() / (double) cuenta_lecciones_totales.get());
} else {
doc.append("proporcion numero lecciones completadas/totales", "No hay lecciones totales. La proporción no se puede calcular");
}
collection.insertOne(doc);

        
       // while (executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
            // Espera a que todos los hilos terminen
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error al procesar usuarios", e);
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
        return request.createResponseBuilder(HttpStatus.OK).body("Operación terminada con éxito").build();
    }  	
    private static void fetchAndPrintUserUnits(OkHttpClient client, String external_user_id, JSONObject user, MongoCollection<Document> collection /*OnUserCompleteCallback callback*/) throws IOException {
    	
        String signature = DigestUtils.sha256Hex(API_SECRET);
        String authHeader = "Voxy " + API_KEY + ":" + signature;

        String userUrl = API_URL + "/" + external_user_id + "/units";
        Request request = new Request.Builder()
                .url(userUrl)
                .method("GET", null)
                .addHeader("Authorization", authHeader)
                .build();
        Response response = client.newCall(request).execute();
        String responseBody = response.body().string();
        try {
            JSONArray units = new JSONArray(responseBody);
            for (int i = 0; i < units.length(); i++) {
                JSONObject unit = units.getJSONObject(i);
                cuenta_unidades_totales.incrementAndGet(); // Incrementar el número total de unidades
              // Creas el mapa aquí para que esté en el mismo ámbito que donde lo usas después
              Map<String, Object> completedlessonsMap = new HashMap<>();

               
                    
                    // Ahora comprobamos el status de las lecciones dentro de la unidad
                JSONObject lessons = unit.getJSONObject("lessons");
                Iterator<String> keys = lessons.keys();

                 
                while(keys.hasNext()) {
                    String key = keys.next();
                    JSONObject lesson = lessons.getJSONObject(key);
                    cuenta_lecciones_totales.incrementAndGet(); // Incrementamos el número total de lecciones
                    if (lesson.getString("status").equals("complete")  && !user.isNull("external_user_id")) {
                        cuenta_lecciones_completadas.incrementAndGet(); // Incrementamos las lecciones completadas
                        System.out.println("   Lección completa en unidad: " + unit.getString("unit_name") + " lección: " + key + " por el usuario: " + external_user_id);
                       // Añadir la lección al mapa de lecciones completas
                completedlessonsMap.put(key, lesson.toMap()); 
                    }
                    
                }
                 // Comprobamos el status de la unidad
                if (unit.getString("status").equals("complete")) {
                	cuenta_unidades_completadas.incrementAndGet(); // Incrementar el número total de unidades completas
                	System.out.println("El usuario " + external_user_id + " ha completado la unidad: " + unit.getString("unit_name"));
                }

                  // Agrega el documento a la base de datos, independientemente del estado de la unidad
    Document doc = new Document("external_user_id", external_user_id)
    .append("unit_name", unit.getString("unit_name"))
    .append("status", unit.getString("status"))
    .append("lessons", completedlessonsMap);
    collection.insertOne(doc);
        } 
    } catch (JSONException ex) {

                
         
                //System.out.println("Response body for: " + external_user_id +":"+ " "+ responseBody);
                
            }
        
        }
         
            
        
      
    private static String createSignature(Map<String, String> params) {
        List<String> keys = new ArrayList<>(params.keySet());
        Collections.sort(keys);
        StringBuilder sb = new StringBuilder(API_SECRET);
        for (String key : keys) {
            sb.append(key).append("=").append(URLEncoder.encode(params.get(key), StandardCharsets.UTF_8));
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
    /**
     * This function listens at endpoint "/api/HttpExample". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/HttpExample
     * 2. curl "{your host}/api/HttpExample?name=HTTP%20Query"
     */