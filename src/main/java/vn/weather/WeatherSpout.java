package vn.weather;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Map;
import java.util.Random;

/**
 * Emits simulated weather by default.
 * Set environment variable WEATHER_SOURCE=api to use Open-Meteo current weather.
 */
public class WeatherSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private final Random random = new Random();
    private final ObjectMapper mapper = new ObjectMapper();
    private HttpClient httpClient;
    private boolean useApi;
    private long lastEmitMillis = 0L;
    private static final long INTERVAL_MS = 5000L;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        this.httpClient = HttpClient.newHttpClient();
        this.useApi = "api".equalsIgnoreCase(System.getenv().getOrDefault("WEATHER_SOURCE", "simulated"));
        System.out.println("[WeatherSpout] source=" + (useApi ? "Open-Meteo API" : "SIMULATED"));
    }

    @Override
    public void nextTuple() {
        long now = System.currentTimeMillis();
        if (now - lastEmitMillis < INTERVAL_MS) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return;
        }
        lastEmitMillis = now;

        try {
            if (useApi) {
                emitFromApi();
            } else {
                emitSimulated();
            }
        } catch (Exception e) {
            System.err.println("[WeatherSpout] Could not obtain weather data: " + e.getMessage());
        }
    }

    private void emitSimulated() {
        double temperature = round(27.0 + random.nextDouble() * 9.0, 1);
        int humidity = 60 + random.nextInt(36);
        double windSpeed = round(random.nextDouble() * 25.0, 1);
        double precipitation = round(random.nextDouble() < 0.25 ? random.nextDouble() * 4.0 : 0.0, 1);
        emit("Ho Chi Minh City", temperature, humidity, windSpeed, precipitation, Instant.now().toString());
    }

    private void emitFromApi() throws Exception {
        String url = "https://api.open-meteo.com/v1/forecast"
                + "?latitude=10.8231&longitude=106.6297"
                + "&current=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m"
                + "&timezone=Asia%2FBangkok";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Open-Meteo HTTP status " + response.statusCode());
        }

        JsonNode root = mapper.readTree(response.body());
        JsonNode current = root.path("current");
        if (current.isMissingNode() || current.isNull()) {
            throw new IllegalStateException("API response does not contain current weather");
        }

        String observedAt = current.path("time").asText(Instant.now().toString());
        emit(
                "Ho Chi Minh City",
                current.path("temperature_2m").asDouble(Double.NaN),
                current.path("relative_humidity_2m").asInt(-1),
                current.path("wind_speed_10m").asDouble(Double.NaN),
                current.path("precipitation").asDouble(Double.NaN),
                observedAt
        );
    }

    private void emit(String city, double temperature, int humidity, double windSpeed,
                      double precipitation, String observedAt) {
        collector.emit(new Values(city, temperature, humidity, windSpeed, precipitation, observedAt));
        System.out.printf("[Spout] emitted city=%s temp=%.1f°C humidity=%d%% rain=%.1fmm%n",
                city, temperature, humidity, precipitation);
    }

    private double round(double value, int places) {
        double scale = Math.pow(10, places);
        return Math.round(value * scale) / scale;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                "city", "temperature", "humidity", "windSpeed", "precipitation", "observedAt"
        ));
    }
}
