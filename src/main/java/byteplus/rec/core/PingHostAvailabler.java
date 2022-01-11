package byteplus.rec.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PingHostAvailabler implements HostAvailabler{
    private static final Duration DEFAULT_PING_INTERVAL = Duration.ofMillis(1000);

    private static final int DEFAULT_WINDOW_SIZE = 60;

    private static final float DEFAULT_FAILURE_RATE_THRESHOLD = (float) 0.1;

    private static final String DEFAULT_PING_URL_FORMAT = "http://%s/predict/api/ping";

    private static final Duration DEFAULT_PING_TIMEOUT = Duration.ofMillis(200);

    private PingHostAvailablerConfig config;
    private List<String> availableHosts;
    private Map<String, Window> hostWindowMap;
    private OkHttpClient httpCli;
    private ScheduledExecutorService executor;

    public PingHostAvailabler(PingHostAvailablerConfig config) {
        this.config = config;
        availableHosts = config.getHosts();
        if (config.getHosts().size() <= 1) {
            return;
        }
        hostWindowMap = new HashMap<>(config.getHosts().size());
        for (String host : config.getHosts()) {
            hostWindowMap.put(host, new Window(config.getWindowSize()));
        }
        httpCli = new OkHttpClient()
                .newBuilder()
                .callTimeout(config.pingTimeout)
                .build();
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(this::checkHost, 0, config.pingInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Data
    @Accessors(chain = true)
    @AllArgsConstructor
    public static class PingHostAvailablerConfig {
        private String pingURLFormat;
        private int windowSize;
        private float failureRateThreshold;
        private Duration pingInterval;
        private Duration pingTimeout;
        private List<String> hosts;

        protected PingHostAvailablerConfig(List<String> hosts) {
            pingURLFormat = DEFAULT_PING_URL_FORMAT;
            windowSize = DEFAULT_WINDOW_SIZE;
            failureRateThreshold = DEFAULT_FAILURE_RATE_THRESHOLD;
            pingInterval = DEFAULT_PING_INTERVAL;
            pingTimeout = DEFAULT_PING_TIMEOUT;
            this.hosts = hosts;
        }
    }

    private void checkHost() {
        try {
            doCheckHost();
        } catch (Throwable e) {
            log.error("[ByteplusSDK] ping find unexpected err, {}", e.getMessage());
        }
    }

    private void doCheckHost() {
        List<String> availableHostsTmp = new ArrayList<>(config.getHosts().size());
        for (String host : config.getHosts()) {
            Window window = hostWindowMap.get(host);
            window.put(doPing(host));
            if (window.failureRate() < config.getFailureRateThreshold()) {
                availableHostsTmp.add(host);
            }
        }
        availableHosts = availableHostsTmp;
        // Make sure that at least have host returns
        if (availableHosts.size() < 1) {
            availableHosts = config.getHosts();
            return;
        }
        if (availableHosts.size() == 1) {
            return;
        }
        availableHosts.sort((host1, host2) -> {
            float host1FailureRate = hostWindowMap.get(host1).failureRate();
            float host2FailureRate = hostWindowMap.get(host2).failureRate();
            float delta = host1FailureRate - host2FailureRate;
            if (delta > 0.0001) {
                return 1;
            } else if (delta < -0.0001) {
                return -1;
            }
            return 0;
        });
    }

    private boolean doPing(String host) {
        String url = String.format(config.pingURLFormat, host);
        Request httpReq = new Request.Builder()
                .url(url)
                .get()
                .build();
        Call httpCall = httpCli.newCall(httpReq);
        long start = System.currentTimeMillis();
        try (Response httpRsp = httpCall.execute()) {
            return httpRsp.code() == 200;
        } catch (Throwable e) {
            log.warn("[ByteplusSDK] ping find err, host:{} err:{}", host, e.getMessage());
            return false;
        } finally {
            long cost = System.currentTimeMillis() - start;
            log.debug("[ByteplusSDK] ping host:'{}' cost:'{}ms'", host, cost);
        }
    }

    @Override
    public List<String> getAvailableHosts() {
        return availableHosts;
    }

    @Override
    public List<String> Hosts() {
        return config.getHosts();
    }

    @Override
    public void setHosts(List<String> hosts) {
        config.setHosts(hosts);
    }

    @Override
    public String GetHost() {
        return availableHosts.get(0);
    }

    @Override
    public void Shutdown() {
        if (Objects.isNull(executor)) {
            return;
        }
        executor.shutdown();
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
