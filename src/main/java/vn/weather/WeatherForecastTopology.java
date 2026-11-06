package vn.weather;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Topology entry point.
 * Use the installed Storm CLI: `storm local` for local testing.
 */
public class WeatherForecastTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("weather-spout", new WeatherSpout(), 1);
        builder.setBolt("weather-clean-bolt", new WeatherCleanBolt(), 1)
                .shuffleGrouping("weather-spout");
        builder.setBolt("weather-predict-bolt", new WeatherPredictBolt(), 1)
                .shuffleGrouping("weather-clean-bolt");

        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(1);
        config.setMessageTimeoutSecs(30);

        // The Storm CLI's `local` command runs this submission against its local cluster.
        // Use `--cluster` only when submitting to a configured remote Storm cluster.
        if (args.length > 0 && "--cluster".equalsIgnoreCase(args[0])) {
            StormSubmitter.submitTopology("weather-forecast-topology", config, builder.createTopology());
        } else {
            StormSubmitter.submitTopology("weather-forecast-topology", config, builder.createTopology());
        }
    }
}
