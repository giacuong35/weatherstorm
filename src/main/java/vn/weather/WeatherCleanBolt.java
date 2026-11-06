package vn.weather;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/** Validates incoming weather values and forwards only plausible records. */
public class WeatherCleanBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String city = input.getStringByField("city");
        double temperature = input.getDoubleByField("temperature");
        int humidity = input.getIntegerByField("humidity");
        double windSpeed = input.getDoubleByField("windSpeed");
        double precipitation = input.getDoubleByField("precipitation");
        String observedAt = input.getStringByField("observedAt");

        boolean valid = Double.isFinite(temperature)
                && temperature >= -50 && temperature <= 60
                && humidity >= 0 && humidity <= 100
                && Double.isFinite(windSpeed) && windSpeed >= 0 && windSpeed <= 300
                && Double.isFinite(precipitation) && precipitation >= 0;

        if (!valid) {
            System.err.println("[CleanBolt] rejected invalid record: " + input);
            collector.ack(input);
            return;
        }

        System.out.println("[CleanBolt] record valid for " + city);
        collector.emit(input, new Values(city, temperature, humidity, windSpeed, precipitation, observedAt));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                "city", "temperature", "humidity", "windSpeed", "precipitation", "observedAt"
        ));
    }
}
