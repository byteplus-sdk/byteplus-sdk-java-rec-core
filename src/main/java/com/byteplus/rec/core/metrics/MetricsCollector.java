package com.byteplus.rec.core.metrics;

import com.byteplus.rec.core.BizException;
import com.byteplus.rec.core.HostAvailabler;
import com.byteplus.rec.core.metrics.protocol.ByteplusRecSdkMetrics.Metric;
import com.byteplus.rec.core.metrics.protocol.ByteplusRecSdkMetrics.MetricMessage;
import com.byteplus.rec.core.metrics.protocol.ByteplusRecSdkMetrics.MetricLog;
import com.byteplus.rec.core.metrics.protocol.ByteplusRecSdkMetrics.MetricLogMessage;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.byteplus.rec.core.metrics.Constant.*;


@Slf4j
public class MetricsCollector {
    private static MetricsCfg metricsCfg;
    private static MetricsReporter metricsReporter;
    private static Queue<Metric> metricsCollector;
    private static Queue<MetricLog> metricsLogCollector;
    private static volatile boolean cleaningMetricsCollector;
    private static volatile boolean cleaningMetricsLogCollector;
    // init func can only exec once
    private static final AtomicBoolean initialed = new AtomicBoolean(false);
    private static ScheduledExecutorService reportExecutor;
    private static volatile HostAvailabler hostAvailabler;

    public static void Init(MetricsCfg metricsConfig, HostAvailabler hostAvailabler) {
        if (initialed.get()) {
            return;
        }
        metricsConfig = fillDefaultConfig(metricsConfig);
        doInit(metricsConfig, hostAvailabler);
    }

    private static MetricsCfg fillDefaultConfig(MetricsCfg metricsConfig) {
        if (Objects.isNull(metricsConfig)) {
            metricsConfig = new MetricsCfg();
        }
        metricsConfig = metricsConfig.toBuilder().build();
        if (Objects.isNull(metricsConfig.httpSchema) || metricsConfig.httpSchema.isEmpty()) {
            metricsConfig.httpSchema = DEFAULT_METRICS_HTTP_SCHEMA;
        }
        if (Objects.isNull(metricsConfig.domain) || metricsConfig.domain.isEmpty()) {
            metricsConfig.domain = DEFAULT_METRICS_DOMAIN;
        }
        if (Objects.isNull(metricsConfig.prefix) || metricsConfig.prefix.isEmpty()) {
            metricsConfig.prefix = DEFAULT_METRICS_PREFIX;
        }
        if (Objects.isNull(metricsConfig.reportInterval) || metricsConfig.reportInterval.isZero()) {
            metricsConfig.reportInterval = DEFAULT_REPORT_INTERVAL;
        }
        if (Objects.isNull(metricsConfig.httpTimeout) || metricsConfig.httpTimeout.isZero()) {
            metricsConfig.httpTimeout = DEFAULT_HTTP_TIMEOUT;
        }
        return metricsConfig;
    }

    public static void Init(MetricsOption... opts) {
        if (initialed.get()) {
            return;
        }
        MetricsCfg metricsConfig = new MetricsCfg();
        // apply options
        for (MetricsOption opt : opts) {
            opt.fill((metricsConfig));
        }
        doInit(metricsConfig, null);
    }

    private static synchronized void doInit(MetricsCfg metricsConfig, HostAvailabler hostAvailabler) {
        if (initialed.get()) {
            return;
        }
        metricsCfg = metricsConfig;
        MetricsCollector.hostAvailabler = hostAvailabler;
        // initialize metrics reporter
        metricsReporter = new MetricsReporter(metricsCfg);
        // initialize metrics collector
        metricsCollector = new ConcurrentLinkedQueue<>();
        metricsLogCollector = new ConcurrentLinkedQueue<>();

        if (!isEnableMetrics() && !isEnableMetricsLog()) {
            initialed.set(true);
            return;
        }
        reportExecutor = Executors.newSingleThreadScheduledExecutor();
        reportExecutor.scheduleAtFixedRate(MetricsCollector::report, metricsCfg.reportInterval.toMillis(),
                metricsCfg.reportInterval.toMillis(), TimeUnit.MILLISECONDS);
        initialed.set(true);
    }

    public static boolean isInitialed() {
        return initialed.get();
    }

    public static boolean isEnableMetrics() {
        if (Objects.isNull(metricsCfg)) {
            return false;
        }
        return metricsCfg.isEnableMetrics();
    }

    public static boolean isEnableMetricsLog() {
        if (Objects.isNull(metricsCfg)) {
            return false;
        }
        return metricsCfg.isEnableMetricsLog();
    }

    public static void emitMetric(String type, String name, long value, String... tagKvs) {
        if (!isEnableMetrics()) {
            return;
        }
        // spin when cleaning collector
        int tryTimes = 0;
        while (cleaningMetricsCollector) {
            try {
                if (tryTimes >= MAX_SPIN_TIMES) {
                    return;
                }
                Thread.sleep(5);
                tryTimes += 1;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (metricsCollector.size() > MAX_METRICS_SIZE) {
            log.debug("[MetricsCollector]: The number of metrics exceeds the limit, the metrics write is rejected");
            return;
        }
        String metricName = name;
        if (metricsCfg.getPrefix().length() > 0) {
            metricName = metricsCfg.getPrefix() + "." + metricName;
        }
        Metric metric = Metric.newBuilder()
                .setType(type)
                .setName(metricName)
                .setValue(value)
                .setTimestamp(System.currentTimeMillis())
                .putAllTags(recoverTags(tagKvs))
                .build();
        metricsCollector.add(metric);
    }

    // recover tagString to origin Tags map
    public static Map<String, String> recoverTags(String... tagKvs) {
        Map<String, String> tags = new HashMap<>();
        for (String tagKv : tagKvs) {
            String[] keyValue = tagKv.split(":", 2);
            if (keyValue.length < 2) {
                continue;
            }
            tags.put(keyValue[0], keyValue[1]);
        }
        return tags;
    }

    public static void emitLog(String logID, String message, String logLevel, Long timestamp) {
        if (!isEnableMetricsLog()) {
            return;
        }
        // spin when cleaning collector
        int tryTimes = 0;
        while (cleaningMetricsLogCollector) {
            try {
                if (tryTimes >= MAX_SPIN_TIMES) {
                    return;
                }
                Thread.sleep(5);
                tryTimes += 1;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (metricsLogCollector.size() > MAX_METRICS_LOG_SIZE) {
            log.debug("[MetricsCollector]: The number of metrics logs exceeds the limit, the metrics log write is rejected");
            return;
        }
        MetricLog metricLog = MetricLog.newBuilder()
                .setId(logID)
                .setMessage(message)
                .setLevel(logLevel)
                .setTimestamp(timestamp)
                .build();
        metricsLogCollector.add(metricLog);
    }

    private static void report() {
        if (isEnableMetrics()) {
            reportMetrics();
        }
        if (isEnableMetricsLog()) {
            reportMetricsLog();
        }
    }

    private static void reportMetrics() {
        if (metricsCollector.size() == 0) {
            return;
        }
        List<Metric> metrics = new ArrayList<>();
        cleaningMetricsCollector = true;
        while (true) {
            Metric metric = metricsCollector.poll();
            if (Objects.isNull(metric)) {
                break;
            }
            metrics.add(metric);
        }
        cleaningMetricsCollector = false;
        doReportMetrics(metrics);
    }

    private static void doReportMetrics(List<Metric> metrics) {
        String url = String.format(METRICS_URL_FORMAT, metricsCfg.getHttpSchema(),
                getDomain(METRICS_PATH));
        MetricMessage metricMessage = MetricMessage
                .newBuilder()
                .addAllMetrics(metrics)
                .build();
        try {
            metricsReporter.report(metricMessage, url);
        } catch (BizException e) {
            log.error("[BytePlusSDK][Metrics] report metrics exception, msg:{}, url:{}", e.getMessage(), url);
        }
    }

    private static String getDomain(String path) {
        if (Objects.nonNull(hostAvailabler)) {
            return hostAvailabler.getHost(path);
        }
        return metricsCfg.getDomain();
    }


    private static void reportMetricsLog() {
        if (metricsLogCollector.size() == 0) {
            return;
        }
        List<MetricLog> metricLogs = new ArrayList<>();
        cleaningMetricsLogCollector = true;
        while (true) {
            MetricLog metricLog = metricsLogCollector.poll();
            if (Objects.isNull(metricLog)) {
                break;
            }
            metricLogs.add(metricLog);
        }
        cleaningMetricsLogCollector = false;
        doReportMetricsLogs(metricLogs);
    }

    private static void doReportMetricsLogs(List<MetricLog> metricsLogs) {
        String url = String.format(METRICS_LOG_URL_FORMAT, metricsCfg.getHttpSchema(),
                getDomain(METRICS_LOG_PATH));
        MetricLogMessage metricLogMessage = MetricLogMessage.
                newBuilder().
                addAllMetricLogs(metricsLogs).
                build();
        try {
            metricsReporter.report(metricLogMessage, url);
        } catch (BizException e) {
            log.error("[BytePlusSDK][Metrics] report metrics log exception, msg:{}, url:{}", e.getMessage(), url);
        }
    }

    @Getter
    @Setter
    @Builder(toBuilder = true)
    @AllArgsConstructor
    public static class MetricsCfg {
        // When metrics are enabled, monitoring metrics will be reported to the byteplus server during use.
        private boolean enableMetrics;
        // When metrics log is enabled, the log will be reported to the byteplus server during use.
        private boolean enableMetricsLog;
        // The address of the byteplus metrics service, will be consistent with the host maintained by hostAvailabler.
        private String domain;
        // The prefix of the Metrics indicator, the default is byteplus.rec.sdk, do not modify.
        private String prefix;
        // Use this httpSchema to report metrics to byteplus server, default is https.
        private String httpSchema;
        // The reporting interval, the default is 15s, if the QPS is high, the reporting interval can be reduced to prevent data loss.
        private Duration reportInterval;
        // Timeout for request reporting.
        private Duration httpTimeout;

        // build default metricsCfg
        public MetricsCfg() {
            this.setEnableMetrics(false);
            this.setEnableMetricsLog(false);
            this.setDomain(DEFAULT_METRICS_DOMAIN);
            this.setPrefix(DEFAULT_METRICS_PREFIX);
            this.setHttpSchema(DEFAULT_METRICS_HTTP_SCHEMA);
            this.setReportInterval(DEFAULT_REPORT_INTERVAL);
            this.setHttpTimeout(DEFAULT_HTTP_TIMEOUT);
        }
    }
}
