# Weather Forecasting with Apache Storm 3.0.0

A standalone Maven project for Windows and JDK 25. It does not modify `storm-starter`.

## Requirements
- JDK 25
- Maven 3.9+
- Apache Storm 3.0.0 already extracted, for example `C:\Users\jakuo\apache-storm-3.0.0`

## Build
From this project folder:
```powershell
mvn clean package
```
Expected JAR:
`target\weather-storm-1.0.0.jar`

## Run with simulated weather
```powershell
$env:WEATHER_SOURCE = "simulated"
cd C:\Users\jakuo\apache-storm-3.0.0
powershell -ExecutionPolicy Bypass -File ".\bin\storm.ps1" local "C:\weather-storm-project\target\weather-storm-1.0.0.jar" vn.weather.WeatherForecastTopology --local-ttl 60
```

Change the path if you extracted the project somewhere else.

## Run with Open-Meteo current weather
Internet access is required. The API is called once about every 5 seconds:
```powershell
$env:WEATHER_SOURCE = "api"
cd C:\Users\jakuo\apache-storm-3.0.0
powershell -ExecutionPolicy Bypass -File ".\bin\storm.ps1" local "C:\weather-storm-project\target\weather-storm-1.0.0.jar" vn.weather.WeatherForecastTopology --local-ttl 60
```
To return to simulated data:
```powershell
$env:WEATHER_SOURCE = "simulated"
```

## Pipeline
`WeatherSpout -> WeatherCleanBolt -> WeatherPredictBolt`

- Spout: simulated records by default; Open-Meteo if `WEATHER_SOURCE=api`.
- Clean Bolt: validates ranges and drops invalid records.
- Predict Bolt: applies a transparent heuristic and prints LOW/MEDIUM/HIGH rain-risk labels.

**Important:** LOW/MEDIUM/HIGH is only a rule-based demonstration, not a trained ML forecast or an official weather warning. The next milestone is to train and evaluate a model on historical hourly data and replace the heuristic.
