package com.byteplus.rec.core;

import com.byteplus.rec.core.Auth.Credential;
import com.alibaba.fastjson.JSON;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import lombok.AccessLevel;
import lombok.Getter;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@Slf4j
@Getter(AccessLevel.PRIVATE)
public class HTTPCaller {
    private final static Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

    private static final String DEFAULT_PING_URL_FORMAT = "https://%s/predict/api/ping";

    private final static OkHttpClient defaultHTTPCli = Utils.buildOkHTTPClient(DEFAULT_TIMEOUT);

    private final Clock clock = Clock.systemDefaultZone();

    private volatile static Map<Duration, OkHttpClient> timeoutHTTPCliMap = new HashMap<>();

    private final String tenantID;

    private boolean useAirAuth;

    private String airAuthToken;

    private Credential authCredential;

    private final boolean keepAlive;

    private final Duration keepAlivePingInterval;

    private final HostAvailabler hostAvailabler;

    private ScheduledExecutorService heartbeatExecutor;

    protected HTTPCaller(String tenantID, String air_auth_token,
                         boolean keepAlive, Duration keepAlivePingInterval, HostAvailabler hostAvailabler) {
        this.useAirAuth = true;
        this.tenantID = tenantID;
        this.airAuthToken = air_auth_token;
        this.keepAlive = keepAlive;
        this.keepAlivePingInterval = keepAlivePingInterval;
        this.hostAvailabler = hostAvailabler;
        if (this.keepAlive) {
            initHeartbeatExecutor(this.keepAlivePingInterval);
        }
    }

    protected HTTPCaller(String tenantID, Credential authCredential,
                         boolean keepAlive, Duration keepAlivePingInterval, HostAvailabler hostAvailabler) {
        this.tenantID = tenantID;
        this.authCredential = authCredential;
        this.keepAlive = keepAlive;
        this.keepAlivePingInterval = keepAlivePingInterval;
        this.hostAvailabler = hostAvailabler;
        if (this.keepAlive) {
            initHeartbeatExecutor(this.keepAlivePingInterval);
        }
    }

    protected void initHeartbeatExecutor(Duration keepAlivePingInterval) {
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        heartbeatExecutor.scheduleAtFixedRate(this::heartbeat, 1,
                keepAlivePingInterval.getSeconds(), TimeUnit.SECONDS);
    }

    private void heartbeat() {
        for(String host: hostAvailabler.getHosts()) {
            Utils.ping(defaultHTTPCli, DEFAULT_PING_URL_FORMAT, host);
            for(OkHttpClient client: timeoutHTTPCliMap.values()) {
                Utils.ping(client, DEFAULT_PING_URL_FORMAT, host);
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
        if (Objects.isNull(options.getRequestID())) {
            String requestID = UUID.randomUUID().toString();
            log.info("[ByteplusSDK] requestID is generated by sdk: '{}'", requestID);
            builder.set("Request-Id", requestID);
        } else {
            builder.set("Request-Id", options.getRequestID());
        }
        if (Objects.nonNull(options.getServerTimeout())) {
            builder.set("Timeout-Millis", options.getServerTimeout().toMillis() + "");
        }
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
            String rspEncoding = response.header("Content-Encoding");
            if (Objects.isNull(rspEncoding) || !rspEncoding.contains("gzip")) {
                return rspBody.bytes();
            }
            return gzipDecompress(rspBody.bytes(), url);
        } catch (IOException e) {
            if (e.getMessage().toLowerCase().contains("timeout")) {
                log.error("[ByteplusSDK] do http request timeout, cost:{}ms msg:{} url:{}",
                        Duration.between(startTime, LocalDateTime.now()).toMillis(), e.getMessage(), url);
                throw new NetException(e.toString());
            }
            log.error("[ByteplusSDK] do http request occur exception, msg:{} url:{}", e.getMessage(), url);
            throw new BizException(e.toString());
        } finally {
            log.debug("[ByteplusSDK] http url:{}, cost:{}ms",
                    url, Duration.between(startTime, LocalDateTime.now()).toMillis());
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
            return defaultHTTPCli;
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
            httpClient = Utils.buildOkHTTPClient(timeout);
            Map<Duration, OkHttpClient> timeoutHTTPCliMapTemp = new HashMap<>(timeoutHTTPCliMap.size());
            timeoutHTTPCliMapTemp.putAll(timeoutHTTPCliMap);
            timeoutHTTPCliMapTemp.put(timeout, httpClient);
            timeoutHTTPCliMap = timeoutHTTPCliMapTemp;
            return httpClient;
        }
    }

    private void logErrHTTPResponse(String url, Response response) throws IOException {
        ResponseBody rspBody = response.body();
        if (Objects.isNull(rspBody)) {
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
        log.error("[ByteplusSDK] http status not 200, url:{} code:{} msg:{} headers:\n{} body:\n{}",
                url, response.code(), response.message(),
                response.headers(), new String(rspBodyBytes, StandardCharsets.UTF_8));
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
        if (Objects.isNull(heartbeatExecutor)) {
            return;
        }
        heartbeatExecutor.shutdown();
    }
}
