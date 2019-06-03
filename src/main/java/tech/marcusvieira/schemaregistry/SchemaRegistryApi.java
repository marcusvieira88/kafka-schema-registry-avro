package tech.marcusvieira.schemaregistry;

import java.io.IOException;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

public class SchemaRegistryApi {

    private final static MediaType SCHEMA_CONTENT = MediaType.parse("application/vnd.schemaregistry.v1+json");
    private final static String CLIENT_SCHEMA = "{\n" +
        " \"schema\": \""
        + "{\\\"type\\\":\\\"record\\\","
        + "\\\"name\\\":\\\"Client\\\","
        + "\\\"namespace\\\":\\\"tech.marcusvieira\\\","
        + "\\\"fields\\\":[{"
            + "\\\"name\\\":\\\"client_id\\\",\\\"type\\\":\\\"string\\\"},"
            + "{\\\"name\\\":\\\"first_name\\\",\\\"type\\\":\\\"string\\\"},"
            + "{\\\"name\\\":\\\"last_name\\\",\\\"type\\\":\\\"string\\\"},"
            + "{\\\"name\\\":\\\"age\\\",\\\"type\\\":[\\\"int\\\",\\\"null\\\"]}]}\""
        + "}";

    public static void main(String... args) throws IOException {

        System.out.println(CLIENT_SCHEMA);
        final OkHttpClient client = new OkHttpClient();

        //POST A NEW SCHEMA
        Request request = new Request.Builder()
            .post(RequestBody.create(SCHEMA_CONTENT, CLIENT_SCHEMA))
            .url("http://localhost:8081/subjects/Client/versions")
            .build();
        String response = client.newCall(request).execute().body().string();
        System.out.println(response);

        //LIST ALL SCHEMAS
        request = new Request.Builder()
            .url("http://localhost:8081/subjects")
            .build();
        response = client.newCall(request).execute().body().string();
        System.out.println(response);

        //SHOW ALL VERSIONS OF CLIENT
        request = new Request.Builder()
            .url("http://localhost:8081/subjects/Client/versions/")
            .build();
        response = client.newCall(request).execute().body().string();
        System.out.println(response);

        //SHOW VERSION 1 OF CLIENT
        request = new Request.Builder()
            .url("http://localhost:8081/subjects/Client/versions/1")
            .build();
        response = client.newCall(request).execute().body().string();
        System.out.println(response);

        //SHOW THE SCHEMA WITH ID 2
        request = new Request.Builder()
            .url("http://localhost:8081/schemas/ids/2")
            .build();
        response = client.newCall(request).execute().body().string();
        System.out.println(response);

        //SHOW THE LATEST VERSION OF CLIENT 2
        request = new Request.Builder()
            .url("http://localhost:8081/subjects/Client/versions/latest")
            .build();
        response = client.newCall(request).execute().body().string();
        System.out.println(response);

        //CHECK IF SCHEMA IS REGISTERED
        request = new Request.Builder()
            .post(RequestBody.create(SCHEMA_CONTENT, CLIENT_SCHEMA))
            .url("http://localhost:8081/subjects/Client")
            .build();
        response = client.newCall(request).execute().body().string();
        System.out.println(response);

        //TEST COMPATIBILITY
        request = new Request.Builder()
            .post(RequestBody.create(SCHEMA_CONTENT, CLIENT_SCHEMA))
            .url("http://localhost:8081/compatibility/subjects/Client/versions/latest")
            .build();
        response = client.newCall(request).execute().body().string();
        System.out.println(response);

        // TOP LEVEL CONFIG
        request = new Request.Builder()
            .url("http://localhost:8081/config")
            .build();
        response = client.newCall(request).execute().body().string();
        System.out.println(response);

        // SET TOP LEVEL CONFIG
        request = new Request.Builder()
            .put(RequestBody.create(SCHEMA_CONTENT, "{\"compatibility\": \"none\"}"))
            .url("http://localhost:8081/config")
            .build();
        response = client.newCall(request).execute().body().string();
        System.out.println(response);

        // SET CONFIG FOR CLIENT
        request = new Request.Builder()
            .put(RequestBody.create(SCHEMA_CONTENT, "{\"compatibility\": \"backward\"}"))
            .url("http://localhost:8081/config/Client")
            .build();
        response = client.newCall(request).execute().body().string();
        System.out.println(response);
    }
}
