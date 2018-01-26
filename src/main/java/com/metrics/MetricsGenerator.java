package com.metrics;

import com.google.gson.Gson;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;

import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MetricsGenerator {

    private static Properties properties;
    private static Gson gson = new Gson();
    private static String delimiter;
    private static List<String> metricsTemplate;
    private static List<String> counterMetrics;
    private static List<String> timerMetrics;
    private static List<String> gaugeMetrics;
    private static List<String> excludedMetrics;
    private static String metricsFormat = "${key}:${value}|${type}";
    private static StatsDIngester statsDIngester;
    public static void init(String filePath) throws Exception {
        try {
            properties = new Properties();
            properties.load(new FileInputStream(Paths.get(filePath).toFile()));
            metricsTemplate = gson.fromJson(properties.getProperty("metrics.template"), List.class);
            delimiter = properties.getProperty("metrics.delimiter");
            if (StringUtils.isEmpty(delimiter)) {
                delimiter = " ";
            }
            excludedMetrics = gson.fromJson(properties.getProperty("metrics.exclude"), List.class);
            counterMetrics = gson.fromJson(properties.getProperty("metrics.counter"), List.class);
            timerMetrics = gson.fromJson(properties.getProperty("metrics.timer"), List.class);
            gaugeMetrics = gson.fromJson(properties.getProperty("metrics.gauge"), List.class);
            statsDIngester = StatsDIngester.getInstance(properties.getProperty("statsd.host"), Integer.valueOf(properties.getProperty("statsd.port")));
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    public static String createStats(String input) {
        Map<String, String> metricsMap = new HashMap<>();
        List<String> metricsStr = Arrays.asList(input.split(delimiter));

        for (int i = 0; i < metricsTemplate.size(); i++) {
            if ("Key".equalsIgnoreCase(metricsTemplate.get(i))) {
                if (isExcluded(metricsStr.get(i))) {
                    return StringUtils.EMPTY;
                }
                metricsMap.put(metricsTemplate.get(i), metricsStr.get(i));
                metricsMap.put("type", typeOfMetric(metricsStr.get(i)));
            } else {
                metricsMap.put(metricsTemplate.get(i), metricsStr.get(i));
            }

        }
        return StrSubstitutor.replace(metricsFormat, metricsMap);
    }

    public static boolean ingestStats(String input) {
        String stats = createStats(input);
        if (StringUtils.isNotEmpty(stats)) {
            return statsDIngester.send(1.0, createStats(input));
        }
        return false;
    }

    private static String typeOfMetric(String metric) {
      //  a > b ? a;c
        String statsType = (timerMetrics.contains(metric) ? "ms" : (gaugeMetrics.contains(metric) ? "g" : (counterMetrics.contains(metric) ? "c" : StringUtils.EMPTY))) ;
        statsType = (StringUtils.isEmpty(statsType) ? (timerMetrics.contains("ALL") ? "ms" : (gaugeMetrics.contains("ALL") ? "g" : "c")) : statsType );
        return statsType;
    }
    private static boolean isExcluded(String key) {
        if (excludedMetrics != null && !excludedMetrics.isEmpty()) {
            return excludedMetrics.parallelStream().anyMatch(metric -> key.matches(metric));
        }
        return false;
    }

}
