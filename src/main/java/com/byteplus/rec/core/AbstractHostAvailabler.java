package com.byteplus.rec.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractHostAvailabler implements HostAvailabler {
    @AllArgsConstructor
    @Getter
    protected static class HostAvailabilityScore {
        private final String host;
        private final double score;

        @Override
        public String toString() {
            return "{" +
                    "host='" + host + '\'' +
                    ", score=" + score +
                    '}';
        }
    }

    private final Clock clock = Clock.systemDefaultZone();

    private String projectID;

    private final List<String> defaultHosts;

    private ScheduledExecutorService executor;

    private ScheduledFuture<?> fetchHostsFromServerFuture;

    private OkHttpClient fetchHostsHTTPClient;

    private volatile Map<String, List<String>> hostConfig;

    public AbstractHostAvailabler(List<String> defaultHosts, boolean initImmediately) throws BizException {
        if (Objects.isNull(defaultHosts) || defaultHosts.isEmpty()) {
            throw new BizException("default hosts are empty");
        }
        this.defaultHosts = defaultHosts;
        if (initImmediately) {
            init();
        }
    }

    public AbstractHostAvailabler(String projectID,
                                  List<String> defaultHosts,
                                  boolean initImmediately) throws BizException {
        if (Objects.isNull(projectID) || projectID.isEmpty()) {
            throw new BizException("project is empty");
        }
        if (Objects.isNull(defaultHosts) || defaultHosts.isEmpty()) {
            throw new BizException("default hosts are empty");
        }
        this.projectID = projectID;
        this.defaultHosts = defaultHosts;
        if (initImmediately) {
            init();
        }
    }

    protected void init() throws BizException {
        this.setHosts(defaultHosts);
        executor = Executors.newSingleThreadScheduledExecutor();
        if (Objects.nonNull(this.projectID)) {
            fetchHostsHTTPClient = Utils.buildOkHTTPClient(Duration.ofSeconds(5));
            fetchHostsFromServer();
            fetchHostsFromServerFuture = executor.
                    scheduleAtFixedRate(this::fetchHostsFromServer, 10, 10, TimeUnit.SECONDS);
        }
        executor.scheduleAtFixedRate(this::scoreAndUpdateHosts, 1, 1, TimeUnit.SECONDS);
    }

    // clear origin host config, and use hosts as default config
    // {
    //   "*": {
    //     "*": ${hosts}
    //   }
    // }
    public void setHosts(List<String> hosts) throws BizException {
        if (Objects.isNull(hosts) || hosts.isEmpty()) {
            throw new BizException("host array is empty");
        }
        stopFetchHostsFromServer();
        doScoreAndUpdateHosts(Collections.singletonMap("*", hosts));
    }

    private void stopFetchHostsFromServer() {
        if (Objects.nonNull(this.fetchHostsFromServerFuture)) {
            this.fetchHostsFromServerFuture.cancel(true);
        }
        if (Objects.nonNull(this.fetchHostsHTTPClient)) {
            this.fetchHostsHTTPClient = null;
        }
    }

    private void fetchHostsFromServer() {
        String host = getHost("*");
        String url = String.format("http://%s/data/api/sdk/host?project_id=%s", host, projectID);
        for (int i = 0; i < 3; i++) {
            Map<String, List<String>> rspHostConfig = doFetchHostsFromServer(url);
            if (Objects.isNull(rspHostConfig)) {
                continue;
            }
            if (isServerHostsNotUpdated(rspHostConfig)) {
                log.debug("[ByteplusSDK] hosts from server are not changed, config: {}", rspHostConfig);
                return;
            }
            if (!rspHostConfig.containsKey("*") || rspHostConfig.get("*").isEmpty()) {
                log.warn("[ByteplusSDK] no default value in hosts from server, config: {}", rspHostConfig);
                return;
            }
            doScoreAndUpdateHosts(rspHostConfig);
            return;
        }
        log.warn("[ByteplusSDK] fetch host from server fail although retried, url: {}", url);
    }

    private Map<String, List<String>> doFetchHostsFromServer(String url) {
        long start = clock.millis();
        Request httpRequest = new Request.Builder()
                .url(url)
                .get()
                .build();
        Call httpCall = fetchHostsHTTPClient.newCall(httpRequest);
        try (Response httpRsp = httpCall.execute()) {
            long cost = clock.millis() - start;
            if (httpRsp.code() == Constant.HTTP_STATUS_NOT_FOUND) {
                log.warn("[ByteplusSDK] fetch host from server return not found status, cost:{}ms", cost);
                return Collections.emptyMap();
            }
            if (httpRsp.code() != Constant.HTTP_STATUS_OK) {
                log.warn("[ByteplusSDK] fetch host from server return not ok status:{} cost:{}ms", httpRsp.code(), cost);
                return null;
            }
            ResponseBody rspBody = httpRsp.body();
            String rspBodyStr = Objects.isNull(rspBody) ? null : new String(rspBody.bytes(), StandardCharsets.UTF_8);
            log.debug("[ByteplusSDK] fetch host from server, cost:{}ms rsp:{}", cost, rspBodyStr);
            if (Objects.nonNull(rspBodyStr) && rspBodyStr.length() > 0) {
                return JSON.parseObject(rspBodyStr, new TypeReference<Map<String, List<String>>>() {
                });
            }
            log.warn("[ByteplusSDK] hosts from server are empty");
            return Collections.emptyMap();
        } catch (Throwable e) {
            long cost = clock.millis() - start;
            log.warn("[ByteplusSDK] fetch host from server fail, url:{} cost:{}ms err:{}", url, cost, e.toString());
            return null;
        }
    }

    private boolean isServerHostsNotUpdated(Map<String, List<String>> newHostConfig) {
        if (newHostConfig.size() != this.hostConfig.size()) {
            return false;
        }
        Set<String> paths = newHostConfig.keySet();
        for (String path : paths) {
            List<String> oldPathHosts = this.hostConfig.get(path);
            List<String> newPathHosts = newHostConfig.get(path);
            if (Objects.isNull(oldPathHosts)) {
                return false;
            }
            if (oldPathHosts.size() != newPathHosts.size()) {
                return false;
            }
            if (!newPathHosts.containsAll(oldPathHosts)) {
                return false;
            }
        }
        return true;
    }

    private void scoreAndUpdateHosts() {
        doScoreAndUpdateHosts(this.hostConfig);
    }

    // path->host_array
    // example:
    // {
    //     "*": ["bytedance.com", "byteplus.com"],
    //     "WriteUsers": ["b-bytedance.com", "b-byteplus.com"],
    //     "Predict": ["c-bytedance.com", "c-byteplus.com"]
    // }
    // default config is required:
    // {
    //   "*": ["bytedance.com", "byteplus.com"]
    // }
    private void doScoreAndUpdateHosts(Map<String, List<String>> hostConfig) {
        List<String> hosts = distinctHosts(hostConfig);
        List<HostAvailabilityScore> newHostScores = doScoreHosts(hosts);
        log.debug("[ByteplusSDK] score hosts result: {}", newHostScores);
        if (Objects.isNull(newHostScores) || newHostScores.isEmpty()) {
            log.error("[ByteplusSDK] scoring hosts return an empty list");
            return;
        }
        Map<String, List<String>> newHostConfig = copyAndSortHost(hostConfig, newHostScores);
        if (isHostConfigNotUpdated(this.hostConfig, newHostConfig)) {
            log.debug("[ByteplusSDK] host order is not changed, {}", newHostConfig);
            return;
        }
        log.warn("[ByteplusSDK] set new host config: {}, old config: {}", newHostConfig, hostConfig);
        this.hostConfig = newHostConfig;
    }

    private List<String> distinctHosts(Map<String, List<String>> hostConfig) {
        Set<String> hostSet = new HashSet<>();
        hostConfig.forEach((path, hosts) -> hostSet.addAll(hosts));
        return new ArrayList<>(hostSet);
    }

    // if host not in result list, will set zero to score
    protected abstract List<HostAvailabilityScore> doScoreHosts(List<String> hosts);

    private Map<String, List<String>> copyAndSortHost(
            Map<String, List<String>> hostConfig, List<HostAvailabilityScore> newHostScores) {

        Map<String, Double> hostScoreIndex = newHostScores.stream()
                .collect(Collectors.toMap(HostAvailabilityScore::getHost, HostAvailabilityScore::getScore));
        Map<String, List<String>> newHostConfig = new HashMap<>();

        hostConfig.forEach((path, hosts) -> {
            List<String> newHosts = new ArrayList<>(hosts);
            // from big to small
            newHosts.sort((x, y) -> {
                double scoreX = hostScoreIndex.getOrDefault(x, 0.0);
                double scoreY = hostScoreIndex.getOrDefault(y, 0.0);
                double delta = scoreX - scoreY;
                if (delta == 0) {
                    return 0;
                }
                return delta > 0 ? -1 : 1;
            });
            newHostConfig.put(path, newHosts);
        });
        return newHostConfig;
    }

    private boolean isHostConfigNotUpdated(
            Map<String, List<String>> oldHostConfig, Map<String, List<String>> newHostConfig) {
        if (Objects.isNull(oldHostConfig)) {
            return false;
        }
        if (Objects.isNull(newHostConfig)) {
            return true;
        }
        Set<String> pathSet = oldHostConfig.keySet();
        for (String path : pathSet) {
            List<String> oldHosts = oldHostConfig.get(path);
            List<String> newHosts = newHostConfig.get(path);
            if (!oldHosts.equals(newHosts)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String getHost(String httpPath) {
        List<String> hosts = hostConfig.get(httpPath);
        if (Objects.isNull(hosts) || hosts.isEmpty()) {
            return hostConfig.get("*").get(0);
        }
        return hosts.get(0);
    }

    @Override
    public void shutdown() {
        if (Objects.isNull(executor)) {
            return;
        }
        executor.shutdown();
    }
}
