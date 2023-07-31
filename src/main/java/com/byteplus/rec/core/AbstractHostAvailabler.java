package com.byteplus.rec.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.byteplus.rec.core.metrics.Metrics;
import com.byteplus.rec.core.metrics.MetricsLog;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
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

    private static final Duration DEFAULT_FETCH_HOST_INTERVAL = Duration.ofSeconds(10);

    private static final Duration DEFAULT_SCORE_HOST_INTERVAL = Duration.ofSeconds(1);

    private final Clock clock = Clock.systemDefaultZone();

    protected String projectID;

    private List<String> defaultHosts;

    private ScheduledExecutorService executor;

    private ScheduledFuture<?> fetchHostsFromServerFuture;

    private OkHttpClient fetchHostsHTTPClient;

    private volatile Map<String, List<String>> hostConfig;

    public AbstractHostAvailabler(List<String> defaultHosts, boolean initImmediately) throws BizException {
        if (Objects.isNull(defaultHosts) || defaultHosts.isEmpty()) {
            throw new BizException("default hosts are empty");
        }
        this.projectID = "";
        this.defaultHosts = defaultHosts;
        if (initImmediately) {
            init(DEFAULT_FETCH_HOST_INTERVAL, DEFAULT_SCORE_HOST_INTERVAL);
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
            init(DEFAULT_FETCH_HOST_INTERVAL, DEFAULT_SCORE_HOST_INTERVAL);
        }
    }

    protected void init(Duration fetchHostInterval, Duration scoreHostInterval) throws BizException {
        this.setHosts(defaultHosts);
        executor = Executors.newSingleThreadScheduledExecutor();
        if (Objects.nonNull(this.projectID) && !this.projectID.isEmpty()) {
            fetchHostsHTTPClient = Utils.buildOkHTTPClient(Duration.ofSeconds(5));
            fetchHostsFromServer();
            fetchHostsFromServerFuture = executor.
                    scheduleAtFixedRate(this::fetchHostsFromServer,
                            fetchHostInterval.toMillis(), fetchHostInterval.toMillis(), TimeUnit.MILLISECONDS);
        }
        executor.scheduleAtFixedRate(this::scoreAndUpdateHosts,
                scoreHostInterval.toMillis(), scoreHostInterval.toMillis(), TimeUnit.MILLISECONDS);
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
        this.defaultHosts = hosts;
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
        String url = String.format("http://%s/data/api/sdk/host?project_id=%s", defaultHosts.get(0), projectID);
        String reqID = "fetch_" + UUID.randomUUID().toString();
        for (int i = 0; i < 3; i++) {
            Map<String, List<String>> rspHostConfig = doFetchHostsFromServer(reqID, url);
            if (Objects.isNull(rspHostConfig)) {
                continue;
            }
            if (isServerHostsNotUpdated(rspHostConfig)) {
                String metricsLogFormat = "[ByteplusSDK][Fetch] hosts from server are not changed," +
                        " project_id:%s, config:%s";
                MetricsLog.info(reqID, metricsLogFormat, projectID, rspHostConfig);
                log.debug("[ByteplusSDK] hosts from server are not changed, config: {}", rspHostConfig);
                return;
            }
            if (!rspHostConfig.containsKey("*") || rspHostConfig.get("*").isEmpty()) {
                String[] metricsTags = new String[]{
                        "type:no_default_hosts",
                        "url:" + Utils.escapeMetricsTagValue(url),
                        "project_id:" + projectID
                };
                Metrics.counter(Constant.METRICS_KEY_COMMON_WARN, 1, metricsTags);
                String metricsLogFormat = "[ByteplusSDK][Fetch] no default value in hosts from server," +
                        " project_id:%s, config:%s";
                MetricsLog.warn(reqID, metricsLogFormat, projectID, rspHostConfig);
                log.warn("[ByteplusSDK] no default value in hosts from server, config: {}", rspHostConfig);
                return;
            }
            doScoreAndUpdateHosts(rspHostConfig);
            return;
        }
        String[] metricsTags = new String[]{
                "type:fetch_host_fail_although_retried",
                "url:" + Utils.escapeMetricsTagValue(url),
                "project_id:" + projectID
        };
        Metrics.counter(Constant.METRICS_KEY_COMMON_ERROR, 1, metricsTags);
        String metricsLogFormat = "[ByteplusSDK][Fetch] fetch host from server fail although retried," +
                " project_id:%s url:%s";
        MetricsLog.warn(reqID, metricsLogFormat, projectID, url);
        log.warn("[ByteplusSDK] fetch host from server fail although retried, url: {}", url);
    }

    private Map<String, List<String>> doFetchHostsFromServer(String reqID,String url) {
        long start = clock.millis();
        Headers headers = new Headers.Builder()
                .set("Request-Id", reqID)
                .set("Project-Id", projectID)
                .build();
        Request httpRequest = new Request.Builder()
                .url(url)
                .headers(headers)
                .get()
                .build();
        Call httpCall = fetchHostsHTTPClient.newCall(httpRequest);
        try (Response httpRsp = httpCall.execute()) {
            long cost = clock.millis() - start;
            if (httpRsp.code() == Constant.HTTP_STATUS_NOT_FOUND) {
                String[] metricsTags = new String[]{
                        "type:fetch_host_status_400",
                        "url:" + Utils.escapeMetricsTagValue(url),
                        "project_id:" + projectID
                };
                Metrics.counter(Constant.METRICS_KEY_COMMON_ERROR, 1, metricsTags);
                String metricsLogFormat = "[ByteplusSDK][Fetch] fetch host from server return not found status," +
                        " project_id:%s cost:%dms";
                MetricsLog.warn(reqID, metricsLogFormat, projectID, cost);
                log.warn("[ByteplusSDK] fetch host from server return not found status, cost:{}ms", cost);
                return Collections.emptyMap();
            }
            if (httpRsp.code() != Constant.HTTP_STATUS_OK) {
                String[] metricsTags = new String[]{
                        "type:fetch_host_not_ok",
                        "url:" + Utils.escapeMetricsTagValue(url),
                        "project_id:" + projectID
                };
                Metrics.counter(Constant.METRICS_KEY_COMMON_ERROR, 1, metricsTags);
                String metricsLogFormat = "[ByteplusSDK][Fetch] fetch host from server return not ok status," +
                        " project_id:%s, status:%d, cost:%dms";
                MetricsLog.warn(reqID, metricsLogFormat, projectID, httpRsp.code(), cost);
                log.warn("[ByteplusSDK] fetch host from server return not ok status:{} cost:{}ms", httpRsp.code(), cost);
                return null;
            }
            ResponseBody rspBody = httpRsp.body();
            String rspBodyStr = Objects.isNull(rspBody) ? null : new String(rspBody.bytes(), StandardCharsets.UTF_8);
            String[] metricsTags = new String[]{
                    "url:" + Utils.escapeMetricsTagValue(url),
                    "project_id:" + projectID
            };
            Metrics.counter(Constant.METRICS_KEY_REQUEST_COUNT, 1, metricsTags);
            Metrics.timer(Constant.METRICS_KEY_REQUEST_TOTAL_COST, cost, metricsTags);
            String metricsLogFormat = "[ByteplusSDK][Fetch] fetch host from server," +
                    " project_id:%s, url:%s, cost:%dms, rsp: %s";
            MetricsLog.info(reqID, metricsLogFormat, projectID, url, cost, rspBodyStr);
            log.debug("[ByteplusSDK] fetch host from server, cost:{}ms rsp:{}", cost, rspBodyStr);
            if (Objects.nonNull(rspBodyStr) && rspBodyStr.length() > 0) {
                return JSON.parseObject(rspBodyStr, new TypeReference<Map<String, List<String>>>() {
                });
            }
            log.warn("[ByteplusSDK] hosts from server are empty");
            return Collections.emptyMap();
        } catch (Throwable e) {
            long cost = clock.millis() - start;
            String[] metricsTags = new String[]{
                    "type:fetch_host_fail",
                    "url:" + Utils.escapeMetricsTagValue(url),
                    "project_id:" + projectID
            };
            Metrics.counter(Constant.METRICS_KEY_COMMON_ERROR, 1, metricsTags);
            String metricsLogFormat = "[ByteplusSDK][Fetch] fetch host from server fail," +
                    " project_id:%s, url:%s, cost:%dms, err: %s";
            MetricsLog.warn(reqID, metricsLogFormat, projectID, url, cost, e.toString());
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
        String logID = "score_" + UUID.randomUUID().toString();
        List<String> hosts = distinctHosts(hostConfig);
        List<HostAvailabilityScore> newHostScores = doScoreHosts(hosts);
        MetricsLog.info(logID, "[ByteplusSDK][Score] score hosts, project_id:%s, result:%s",
                projectID, newHostScores);
        log.debug("[ByteplusSDK] score hosts result: {}", newHostScores);
        if (Objects.isNull(newHostScores) || newHostScores.isEmpty()) {
            String[] metricsTags = new String[]{
                    "type:scoring_hosts_return_empty_list",
                    "project_id:" + projectID
            };
            Metrics.counter(Constant.METRICS_KEY_COMMON_ERROR, 1, metricsTags);
            MetricsLog.error(logID, "[ByteplusSDK][Score] scoring hosts return an empty list, project_id:%s", projectID);
            log.error("[ByteplusSDK] scoring hosts return an empty list");
            return;
        }
        Map<String, List<String>> newHostConfig = copyAndSortHost(hostConfig, newHostScores);
        if (isHostConfigNotUpdated(this.hostConfig, newHostConfig)) {
            MetricsLog.info(logID, "[ByteplusSDK][Score] host order is not changed, project_id:%s, hosts:%s",
                    projectID, newHostScores);
            log.debug("[ByteplusSDK] host order is not changed, {}", newHostConfig);
            return;
        }
        String[] metricsTags = new String[]{
                "type:set_new_host_config",
                "project_id:" + projectID
        };
        Metrics.counter(Constant.METRICS_KEY_COMMON_INFO, 1, metricsTags);
        MetricsLog.info(logID, "[ByteplusSDK][Score] set new host config: %s, old config: %s, project_id: %s",
                newHostConfig, hostConfig, projectID);
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
    public List<String> getHosts() {
        return distinctHosts(hostConfig);
    }

    @Override
    public void shutdown() {
        if (Objects.isNull(executor)) {
            return;
        }
        executor.shutdown();
    }
}
