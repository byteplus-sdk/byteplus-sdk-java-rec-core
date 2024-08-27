package com.byteplus.rec.core;

import com.byteplus.rec.core.Auth.Credential;
import com.alibaba.fastjson.JSON;
import com.byteplus.rec.core.metrics.Metrics;
import com.byteplus.rec.core.metrics.MetricsLog;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@Slf4j
@Getter(AccessLevel.PRIVATE)
public class HTTPCaller {
    private final static Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

    private static final String DEFAULT_PING_URL_FORMAT = "%s://%s/predict/api/ping";

    private final Clock clock = Clock.systemDefaultZone();

    private volatile Map<Duration, OkHttpClient> timeoutHTTPCliMap = new HashMap<>();

    private final ThreadLocal<String> requestID = new ThreadLocal<>();

    private final String projectID;

    private final String tenantID;

    private boolean useAirAuth;

    private String airAuthToken;

    private Credential authCredential;

    private final HostAvailabler hostAvailabler;

    private final Config config;

    private final String schema;

    private final boolean keepAlive;

    private ScheduledExecutorService heartbeatExecutor;

    private ExecutorService keepAliveExecutor;

    protected HTTPCaller(String projectID, String tenantID, String air_auth_token,
                         HostAvailabler hostAvailabler, Config callerConfig, String schema, boolean keepAlive) {
        this.config = fillDefaultConfig(callerConfig);
        this.useAirAuth = true;
        this.projectID = projectID;
        this.tenantID = tenantID;
        this.airAuthToken = air_auth_token;
        this.hostAvailabler = hostAvailabler;
        this.schema = schema;
        this.keepAlive = keepAlive;
        if (this.keepAlive) {
            initHeartbeatExecutor(this.config.getKeepAlivePingInterval());
        }
    }

    protected HTTPCaller(String projectID, String tenantID, Credential authCredential,
                         HostAvailabler hostAvailabler, Config callerConfig, String schema, boolean keepAlive) {
        this.config = fillDefaultConfig(callerConfig);
        this.projectID = projectID;
        this.tenantID = tenantID;
        this.authCredential = authCredential;
        this.hostAvailabler = hostAvailabler;
        this.schema = schema;
        this.keepAlive = keepAlive;
        if (this.keepAlive) {
            initHeartbeatExecutor(this.config.getKeepAlivePingInterval());
        }
    }

    private String getReqID() {
        return this.requestID.get();
    }

    private Config fillDefaultConfig(Config config) {
        config = config.toBuilder().build();
        if (config.maxIdleConnections <= 0) {
            config.maxIdleConnections = Constant.DEFAULT_MAX_IDLE_CONNECTIONS;
        }
        if (Objects.isNull(config.keepAliveDuration) || config.keepAliveDuration.isZero()) {
            config.keepAliveDuration = Constant.DEFAULT_KEEPALIVE_DURATION;
        }
        if (Objects.isNull(config.keepAlivePingInterval) || config.keepAlivePingInterval.isZero()) {
            config.keepAlivePingInterval = Constant.DEFAULT_KEEPALIVE_PING_INTERVAL;
        }
        if (config.maxKeepAliveConnections <= 0) {
            config.maxKeepAliveConnections = Constant.DEFAULT_MAX_KEEPALIVE_CONNECTIONS;
        }
        return config;
    }

    protected void initHeartbeatExecutor(Duration keepAlivePingInterval) {
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        keepAliveExecutor = Executors.newFixedThreadPool(this.config.maxKeepAliveConnections);
        heartbeatExecutor.scheduleAtFixedRate(this::heartbeat, 1,
                keepAlivePingInterval.getSeconds(), TimeUnit.SECONDS);
    }

    private void heartbeat() {
        for(String host: hostAvailabler.getHosts()) {
            for(Map.Entry<Duration, OkHttpClient> entry: timeoutHTTPCliMap.entrySet()) {
                long timeoutMs = entry.getKey().toMillis();
                OkHttpClient client = entry.getValue();
                for (int i = 0; i < config.maxKeepAliveConnections; i++) {
                   keepAliveExecutor.submit(new Runnable() {
                       @Override
                       public void run() {
                           String[] metricsTags = new String[] {
                                   "from:http_caller",
                                   "project_id:" + getProjectID(),
                                   "timeout:" + timeoutMs,
                                   "host:" + Utils.escapeMetricsTagValue(host)
                           };
                           Metrics.counter(Constant.METRICS_KEY_HEARTBEAT_COUNT, 1, metricsTags);
                           Utils.ping(getProjectID(), client, DEFAULT_PING_URL_FORMAT, schema, host);
                       }
                   });
                }
            }
        }
    }

    protected <Rsp extends Message, Req extends Message> Rsp doPBRequest(
            String url,
            Req request,
            Parser<Rsp> rspParser,
            Options options) throws NetException, BizException {
        byte[] reqBytes = request.toByteArray();
        String contentType = "application/x-protobuf";
        byte[] rspBytes = doRequest(url, reqBytes, contentType, options);
        try {
            return rspParser.parseFrom(rspBytes);
        } catch (InvalidProtocolBufferException e) {
            String[] metricsTags = new String[]{
                    "type:parse_response_fail",
                    "project_id:" + getProjectID()
            };
            Metrics.counter(Constant.METRICS_KEY_COMMON_ERROR, 1, metricsTags);
            MetricsLog.error(getReqID(),"[ByteplusSDK]parse response fail, project_id:%s, url:%s err:%s ",
                    getProjectID(), url, e.getMessage());
            log.error("[ByteplusSDK]parse response fail, url:{} err:{} ", url, e.getMessage());
            throw new BizException("parse response fail");
        }
    }

    protected <Rsp> Rsp doJSONRequest(
            String url,
            Object request,
            Rsp resp,
            Options options) throws NetException, BizException {
        byte[] reqBytes = JSON.toJSONBytes(request);
        String contentType = "application/json";
        byte[] rspBytes = doRequest(url, reqBytes, contentType, options);
        return JSON.parseObject(rspBytes, resp.getClass());
    }

    private byte[] doRequest(String url,
                             byte[] reqBytes,
                             String contentType,
                             Options options) throws NetException, BizException {
        reqBytes = gzipCompress(reqBytes);
        Headers headers = buildHeaders(options, contentType);
        url = buildUrlWithQueries(options, url);
        return doHTTPRequest(url, headers, reqBytes, options.getTimeout());
    }

    private byte[] gzipCompress(byte[] bodyBytes) {
        if (bodyBytes == null || bodyBytes.length == 0) {
            return new byte[0];
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip;
        try {
            gzip = new GZIPOutputStream(out);
            gzip.write(bodyBytes);
            gzip.finish();
            gzip.close();
        } catch (IOException e) {
            log.error("[ByteplusSDK] gzip compress http request bytes error {}", e.getMessage());
            return bodyBytes;
        }
        return out.toByteArray();
    }

    private Headers buildHeaders(Options options, String contentType) {
        Headers.Builder builder = new Headers.Builder();
        builder.set("Content-Encoding", "gzip");
        builder.set("Accept-Encoding", "gzip");
        builder.set("Content-Type", contentType);
        builder.set("Accept", contentType);
        builder.set("Tenant-Id", getTenantID());
        // for metrics
        builder.set("Project-Id", getProjectID());
        withOptionHeaders(builder, options);
        return builder.build();
    }

    private String buildUrlWithQueries(Options options, String url) {
        Map<String, String> queries = new HashMap<>();
        if (Objects.nonNull(options.getQueries())) {
            queries.putAll(options.getQueries());
        }
        if (queries.isEmpty()) {
            return url;
        }
        ArrayList<String> queryParts = new ArrayList<>();
        queries.forEach((queryName, queryValue) ->
                queryParts.add(queryName + "=" + queryValue));
        String queryString = String.join("&", queryParts);
        if (url.contains("?")) { //already contains queries
            return url + "&" + queryString;
        } else {
            return url + "?" + queryString;
        }
    }

    private void withOptionHeaders(Headers.Builder builder, Options options) {
        if (Objects.nonNull(options.getHeaders())) {
            options.getHeaders().forEach(builder::set);
        }
        String requestID = options.getRequestID();
        if (Objects.isNull(requestID)) {
            requestID = UUID.randomUUID().toString();
            log.info("[ByteplusSDK] requestID is generated by sdk: '{}'", requestID);
            builder.set("Request-Id", requestID);
        } else {
            builder.set("Request-Id", options.getRequestID());
        }
        if (Objects.nonNull(options.getServerTimeout())) {
            builder.set("Timeout-Millis", options.getServerTimeout().toMillis() + "");
        }
        this.requestID.set(requestID);
    }

    private String calSignature(byte[] httpBody, String ts, String nonce) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException ignored) {
            return "";
        }
        // Splice in the order of "token", "HttpBody", "tenant_id", "ts", and "nonce".
        // The order must not be mistaken.
        // String need to be encoded as byte arrays by UTF-8
        digest.update(this.getAirAuthToken().getBytes(StandardCharsets.UTF_8));
        digest.update(httpBody);
        digest.update(getTenantID().getBytes(StandardCharsets.UTF_8));
        digest.update(ts.getBytes(StandardCharsets.UTF_8));
        digest.update(nonce.getBytes(StandardCharsets.UTF_8));
        return Utils.bytes2Hex(digest.digest());
    }


    private byte[] doHTTPRequest(String url,
                                 Headers headers,
                                 byte[] bodyBytes,
                                 Duration timeout) throws NetException, BizException {
        long start = System.currentTimeMillis();
        Request request = new Request.Builder()
                .url(url)
                .headers(headers)
                .post(RequestBody.create(bodyBytes))
                .build();
        // append auth headers
        headers = withAuthHeaders(request, bodyBytes);
        request = request.newBuilder().headers(headers).build();
        log.debug("[ByteplusSDK][HTTPCaller] URL:{} Request Headers:\n{}", url, request.headers());
        Call call = selectHTTPClient(timeout).newCall(request);
        LocalDateTime startTime = LocalDateTime.now();
        try (Response response = call.execute()) {
            ResponseBody rspBody = response.body();
            if (response.code() != Constant.HTTP_STATUS_OK) {
                logErrHTTPResponse(url, response);
                throw new BizException(response.message());
            }
            if (Objects.isNull(rspBody)) {
                return null;
            }
            long cost = response.receivedResponseAtMillis() - response.sentRequestAtMillis();
            String[] metricsTags = new String[]{
                    "url:" + Utils.escapeMetricsTagValue(url),
                    "project_id:" + getProjectID()
            };
            Metrics.timer(Constant.METRICS_KEY_REQUEST_COST, cost, metricsTags);
            String metricsLogFormat = "[ByteplusSDK][HTTPCaller] project_id:%s, sent:%d, received:%d, cost:%d, start:%d, end:%d," +
                    " start->sent: %d, connection count:%d, header:%s";
            MetricsLog.info(getReqID(), metricsLogFormat,
                    getProjectID(),
                    response.sentRequestAtMillis(), response.receivedResponseAtMillis(),
                    response.receivedResponseAtMillis() - response.sentRequestAtMillis(),
                    start,
                    System.currentTimeMillis(),
                    response.sentRequestAtMillis() - start,
                    selectHTTPClient(timeout).connectionPool().connectionCount(),
                    response.headers()
            );
            String rspEncoding = response.header("Content-Encoding");
            if (Objects.isNull(rspEncoding) || !rspEncoding.contains("gzip")) {
                return rspBody.bytes();
            }
            return gzipDecompress(rspBody.bytes(), url);
        } catch (IOException e) {
            if (e.getMessage().toLowerCase().contains("timeout")) {
                long cost = Duration.between(startTime, LocalDateTime.now()).toMillis();
                String[] metricsTags = new String[]{
                        "type:request_timeout",
                        "url:" + Utils.escapeMetricsTagValue(url),
                        "project_id:" + getProjectID()
                };
                Metrics.counter(Constant.METRICS_KEY_COMMON_ERROR, 1, metricsTags);
                String metricsLogFormat = "[ByteplusSDK] do http request timeout, project_id:%s, cost:%dms, msg:%s, url:%s";
                MetricsLog.error(getReqID(), metricsLogFormat, getProjectID(), cost, e.getMessage(), url);
                log.error("[ByteplusSDK] do http request timeout, cost:{}ms msg:{} url:{}", cost, e.getMessage(), url);
                throw new NetException(e.toString());
            }
            String[] metricsTags = new String[]{
                    "type:request_occur_exception",
                    "url:" + Utils.escapeMetricsTagValue(url),
                    "project_id:" + getProjectID()
            };
            Metrics.counter(Constant.METRICS_KEY_COMMON_ERROR, 1, metricsTags);
            String metricsLogFormat = "[ByteplusSDK] do http request occur exception, project_id:%s, msg:%s, url:%s";
            MetricsLog.error(getReqID(), metricsLogFormat, getProjectID(), e.getMessage(), url);
            log.error("[ByteplusSDK] do http request occur exception, msg:{} url:{}", e.getMessage(), url);
            throw new BizException(e.toString());
        } finally {
            String[] metricsTags = new String[]{
                    "project_id:" + getProjectID(),
                    "url:" + Utils.escapeMetricsTagValue(url)
            };
            long cost = Duration.between(startTime, LocalDateTime.now()).toMillis();
            Metrics.timer(Constant.METRICS_KEY_REQUEST_TOTAL_COST, cost, metricsTags);
            Metrics.counter(Constant.METRICS_KEY_REQUEST_COUNT, 1, metricsTags);
            MetricsLog.info(getReqID(), "[ByteplusSDK] http request, project_id:%s, http url:%s, cost:%dms",
                    getProjectID(), url, cost);
            log.debug("[ByteplusSDK] http url:{}, cost:{}ms", url, cost);
        }
    }

    private Headers withAuthHeaders(Request request, byte[] bodyBytes) throws BizException {
        if (useAirAuth) {
            Headers originHeaders = request.headers();
            return withAirAuthHeaders(originHeaders, bodyBytes);
        }
        try {
            return Auth.sign(request, bodyBytes, this.getAuthCredential());
        } catch (Exception e) {
            throw new BizException(e.getMessage());
        }
    }

    private Headers withAirAuthHeaders(Headers originHeaders, byte[] reqBytes) {
        // Gets the second-level timestamp of the current time.
        // The server only supports the second-level timestamp.
        // The 'ts' must be the current time.
        // When current time exceeds a certain time, such as 5 seconds, of 'ts',
        // the signature will be invalid and cannot pass authentication
        String ts = "" + (clock.millis() / 1000);
        // Use sub string of UUID as "nonce",  too long will be wasted.
        // You can also use 'ts' as' nonce'
        String nonce = UUID.randomUUID().toString().substring(0, 8);
        // calculate the authentication signature
        String signature = calSignature(reqBytes, ts, nonce);
        return originHeaders.newBuilder()
                .set("Tenant-Id", getTenantID())
                .set("Tenant-Ts", ts)
                .set("Tenant-Nonce", nonce)
                .set("Tenant-Signature", signature)
                .build();
    }

    private OkHttpClient selectHTTPClient(Duration timeout) {
        if (Objects.isNull(timeout) || timeout.isZero()) {
            timeout = DEFAULT_TIMEOUT;
        }
        OkHttpClient httpClient = timeoutHTTPCliMap.get(timeout);
        if (Objects.nonNull(httpClient)) {
            return httpClient;
        }
        synchronized (HTTPCaller.class) {
            httpClient = timeoutHTTPCliMap.get(timeout);
            if (Objects.nonNull(httpClient)) {
                return httpClient;
            }
            httpClient = Utils.buildOkHTTPClient(timeout, config.maxIdleConnections, config.keepAliveDuration);
            Map<Duration, OkHttpClient> timeoutHTTPCliMapTemp = new HashMap<>(timeoutHTTPCliMap.size());
            timeoutHTTPCliMapTemp.putAll(timeoutHTTPCliMap);
            timeoutHTTPCliMapTemp.put(timeout, httpClient);
            timeoutHTTPCliMap = timeoutHTTPCliMapTemp;
            return httpClient;
        }
    }

    private void logErrHTTPResponse(String url, Response response) throws IOException {
        String[] metricsTags = new String[]{
                "type:rsp_status_not_ok",
                "url:" + Utils.escapeMetricsTagValue(url),
                "project_id:" + getProjectID(),
                "status:" + response.code(),
        };
        Metrics.counter(Constant.METRICS_KEY_COMMON_ERROR, 1, metricsTags);
        ResponseBody rspBody = response.body();
        if (Objects.isNull(rspBody)) {
            String logFormat = "[ByteplusSDK] http status not 200, project_id:%s, url:%s, code:%d, msg:%s, headers:\\n%s";
            MetricsLog.error(getReqID(), logFormat,
                    getProjectID(), url, response.code(), response.message(), response.headers());
            log.error("[ByteplusSDK] http status not 200, url:{} code:{} msg:{} headers:\n{}",
                    url, response.code(), response.message(), response.headers());
            return;
        }
        String rspEncoding = response.header("Content-Encoding");
        byte[] rspBodyBytes;
        if (Objects.isNull(rspEncoding) || !rspEncoding.contains("gzip")) {
            rspBodyBytes = rspBody.bytes();
        } else {
            rspBodyBytes = gzipDecompress(rspBody.bytes(), url);
        }
        String bodyStr = new String(rspBodyBytes, StandardCharsets.UTF_8);
        String logFormat = "[ByteplusSDK] http status not 200, project_id:%s, url:%s, code:%d, msg:%s, headers:\\n%s, body:\n%s";
        MetricsLog.error(getReqID(), logFormat,
                getProjectID(), url, response.code(), response.message(), response.headers(), bodyStr);
        log.error("[ByteplusSDK] http status not 200, url:{} code:{} msg:{} headers:\n{} body:\n{}",
                url, response.code(), response.message(), response.headers(), bodyStr);
    }

    private byte[] gzipDecompress(byte[] bodyBytes, String url) {
        if (bodyBytes == null || bodyBytes.length == 0) {
            return new byte[0];
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream in = new ByteArrayInputStream(bodyBytes);
        try {
            GZIPInputStream ungzip = new GZIPInputStream(in);
            byte[] buffer = new byte[256];
            int n;
            while ((n = ungzip.read(buffer)) >= 0) {
                out.write(buffer, 0, n);
            }
        } catch (Exception e) {
            log.error("[ByteplusSDK] gzip decompress http response error, msg:{} url:{}",
                    e.getMessage(), url);
        }
        return out.toByteArray();
    }

    public void shutdown() {
        if (!Objects.isNull(heartbeatExecutor)) {
            heartbeatExecutor.shutdown();
        }
        if (!Objects.isNull(keepAliveExecutor)) {
            keepAliveExecutor.shutdown();
        }
    }

    @Getter
    @Builder(toBuilder = true)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Config {
        // for OkHTTP
        private int maxIdleConnections;

        private Duration keepAliveDuration;

        // for httpCaller.
        private Duration keepAlivePingInterval;

        // for httpCaller.
        private int maxKeepAliveConnections;
    }

    protected static Config getDefaultConfig() {
        return new Config(Constant.DEFAULT_MAX_IDLE_CONNECTIONS,
                Constant.DEFAULT_KEEPALIVE_DURATION,
                Constant.DEFAULT_KEEPALIVE_PING_INTERVAL,
                Constant.DEFAULT_MAX_KEEPALIVE_CONNECTIONS);
    }
}
