package com.byteplus.rec.core;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
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

    private static final String DEFAULT_PING_URL_FORMAT = "http://%s/predict/api/ping";

    private static final Duration DEFAULT_PING_TIMEOUT = Duration.ofMillis(300);

    private final Clock clock = Clock.systemDefaultZone();

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
        init();
    }

    public PingHostAvailabler(String projectID, List<String> hosts) throws BizException {
        this(projectID, hosts, new Config());
    }

    public PingHostAvailabler(String projectID, List<String> hosts, Config config) throws BizException {
        super(projectID, hosts, config.closeFetchHostsFromServer, false);
        this.config = fillDefaultConfig(config);
        httpCli = Utils.buildOkHTTPClient(this.config.pingTimeout);
        init();
    }

    private Config fillDefaultConfig(Config config) {
        config = config.toBuilder().build();
        if (Objects.isNull(config.pingURLFormat)) {
            config.pingURLFormat = DEFAULT_PING_URL_FORMAT;
        }
        if (Objects.isNull(config.pingTimeout)) {
            config.pingTimeout = DEFAULT_PING_TIMEOUT;
        }
        if (config.windowSize <= 0) {
            config.windowSize = DEFAULT_WINDOW_SIZE;
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
            window.put(doPing(host));
        }
        return hosts.stream()
                .map(host -> {
                    double score = 1 - hostWindowMap.get(host).failureRate();
                    return new HostAvailabilityScore(host, score);
                })
                .collect(Collectors.toList());
    }

    private boolean doPing(String host) {
        String url = String.format(config.getPingURLFormat(), host);
        Headers.Builder builder = new Headers.Builder();
        Headers headers = builder.build();
        Request httpReq = new Request.Builder()
                .url(url)
                .headers(headers)
                .get()
                .build();
        Call httpCall = httpCli.newCall(httpReq);
        long start = clock.millis();
        try (Response httpRsp = httpCall.execute()) {
            long cost = clock.millis() - start;
            if (isPingSuccess(httpRsp)) {
                log.debug("[ByteplusSDK] ping success, host:{} cost:{}ms", host, cost);
                return true;
            }
            log.warn("[ByteplusSDK] ping fail, host:{} cost:{}ms status:{}", host, cost, httpRsp.code());
            return false;
        } catch (Throwable e) {
            long cost = clock.millis() - start;
            log.warn("[ByteplusSDK] ping find err, host:'{}' cost:{}ms err:'{}'", host, cost, e.toString());
            return false;
        }
    }

    private boolean isPingSuccess(Response httpRsp) throws IOException {
        if (httpRsp.code() != Constant.HTTP_STATUS_OK) {
            return false;
        }
        ResponseBody rspBody = httpRsp.body();
        if (Objects.isNull(rspBody)) {
            return false;
        }
        String rspStr = new String(rspBody.bytes(), StandardCharsets.UTF_8);
        return rspStr.length() < 20 && rspStr.contains("pong");
    }

    @Getter
    @Builder(toBuilder = true)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Config {
        private String pingURLFormat;

        private Duration pingTimeout;

        private int windowSize;

        boolean closeFetchHostsFromServer;
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
