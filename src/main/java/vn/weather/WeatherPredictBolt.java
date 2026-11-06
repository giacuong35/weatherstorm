package vn.weather;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * First baseline only: a transparent heuristic, not a trained ML model
 * and not an authoritative meteorological forecast.
 */
public class WeatherPredictBolt extends BaseRichBolt {
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

        String risk;
        String explanation;

        if (precipitation >= 1.0 || (humidity >= 90 && windSpeed >= 15.0)) {
            risk = "HIGH";
            explanation = "rain is present or humidity and wind are both high";
        } else if (precipitation > 0.0 || humidity >= 80 || windSpeed >= 25.0) {
            risk = "MEDIUM";
            explanation = "one or more weather indicators are elevated";
        } else {
            risk = "LOW";
            explanation = "no strong rain indicators in this simple baseline";
        }

        System.out.printf(
                "[PREDICTION] city=%s time=%s temp=%.1f°C humidity=%d%% wind=%.1f km/h rain=%.1f mm risk=%s (%s)%n",
                city, observedAt, temperature, humidity, windSpeed, precipitation, risk, explanation
        );

        collector.emit(input, new Values(
                city, temperature, humidity, windSpeed, precipitation, observedAt, risk, explanation
        ));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                "city", "temperature", "humidity", "windSpeed", "precipitation",
                "observedAt", "rainRisk", "explanation"
        ));
    }
}
