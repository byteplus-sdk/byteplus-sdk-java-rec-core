package com.byteplus.rec.core;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class PingHostAvailabler extends AbstractHostAvailabler {
    private static final int DEFAULT_WINDOW_SIZE = 60;

    private static final String DEFAULT_PING_SCHEMA = "http";

    private static final String DEFAULT_PING_URL_FORMAT = "%s://%s/predict/api/ping";

    private static final Duration DEFAULT_PING_TIMEOUT = Duration.ofMillis(300);

    private static final Duration DEFAULT_PING_INTERVAL = Duration.ofSeconds(1);

    private static final Duration DEFAULT_FETCH_HOST_INTERVAL = Duration.ofSeconds(10);

    private final Config config;

    private final Map<String, Window> hostWindowMap = new HashMap<>();

    private final OkHttpClient httpCli;

    public PingHostAvailabler(List<String> hosts) throws BizException {
        this(hosts, new Config());
    }

    public PingHostAvailabler(List<String> hosts, Config config) throws BizException {
        super(hosts, false);
        this.config = fillDefaultConfig(config);
        httpCli = Utils.buildOkHTTPClient(this.config.pingTimeout);
        init(this.config.fetchHostInterval, this.config.pingInterval);
    }

    public PingHostAvailabler(String projectID, List<String> hosts) throws BizException {
        this(projectID, hosts, new Config());
    }

    public PingHostAvailabler(String projectID, List<String> hosts, Config config) throws BizException {
        super(projectID, hosts, false);
        this.config = fillDefaultConfig(config);
        httpCli = Utils.buildOkHTTPClient(this.config.pingTimeout);
        init(this.config.fetchHostInterval, this.config.pingInterval);
    }

    private Config fillDefaultConfig(Config config) {
        if (Objects.isNull(config)) {
            config = new Config();
        }
        config = config.toBuilder().build();
        if (Objects.isNull(config.pingURLFormat)) {
            config.pingURLFormat = DEFAULT_PING_URL_FORMAT;
        }
        if (Objects.isNull(config.pingTimeout) || config.pingTimeout.isZero()) {
            config.pingTimeout = DEFAULT_PING_TIMEOUT;
        }
        if (config.windowSize <= 0) {
            config.windowSize = DEFAULT_WINDOW_SIZE;
        }
        if (Objects.isNull(config.pingInterval) || config.pingInterval.isZero()) {
            config.pingInterval = DEFAULT_PING_INTERVAL;
        }
        if (Objects.isNull(config.fetchHostInterval) || config.fetchHostInterval.isZero()) {
            config.fetchHostInterval = DEFAULT_FETCH_HOST_INTERVAL;
        }
        return config;
    }

    @Override
    protected List<HostAvailabilityScore> doScoreHosts(List<String> hosts) {
        log.debug("[ByteplusSDK] do score hosts:{}", hosts);
        if (hosts.size() == 1) {
            return Collections.singletonList(new HostAvailabilityScore(hosts.get(0), 0.0));
        }
        for (String host : hosts) {
            Window window = hostWindowMap.get(host);
            if (Objects.isNull(window)) {
                window = new Window(config.windowSize);
                hostWindowMap.put(host, window);
            }
            window.put(Utils.ping(projectID, httpCli, config.getPingURLFormat(), DEFAULT_PING_SCHEMA, host));
        }
        return hosts.stream()
                .map(host -> {
                    double score = 1 - hostWindowMap.get(host).failureRate();
                    return new HostAvailabilityScore(host, score);
                })
                .collect(Collectors.toList());
    }

    @Getter
    @Builder(toBuilder = true)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Config {
        private String pingURLFormat;

        private Duration pingTimeout;

        private Duration pingInterval;

        private Duration fetchHostInterval;

        private int windowSize;
    }

    private static class Window {

        private final int size;

        private final boolean[] items;

        private int head;

        private int tail = 0;

        private float failureCount = 0;

        private Window(int size) {
            this.size = size;
            this.head = size - 1;
            items = new boolean[size];
            Arrays.fill(items, true);
        }

        void put(boolean success) {
            if (!success) {
                failureCount++;
            }
            head = (head + 1) % size;
            items[head] = success;
            tail = (tail + 1) % size;
            boolean removingItem = items[tail];
            if (!removingItem) {
                failureCount--;
            }
        }

        float failureRate() {
            return failureCount / (float) size;
        }
    }
}
