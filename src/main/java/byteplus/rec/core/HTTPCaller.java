package byteplus.rec.core;

import byteplus.rec.core.VoclAuth.Credential;
import byteplus.rec.core.VoclAuth;
import com.alibaba.fastjson.JSON;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@Slf4j
@Getter(AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class HTTPCaller {
    // The http request was executed successfully without any net exception
    private final static int SUCCESS_HTTP_CODE = 200;

    private final static OkHttpClient defaultHTTPCli = new OkHttpClient.Builder().build();

    private volatile static Map<Duration, OkHttpClient> timeoutHTTPCliMap = new HashMap<>();

    private String tenantID;

    private String token;

    private boolean useAirAuth;

    private String hostHeader;

    private Credential credential;

    protected <Rsp extends Message, Req extends Message> Rsp doPbRequest(
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

    protected <Rsp extends Object> Rsp doJsonRequest(
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
        if (Objects.nonNull(hostHeader) && hostHeader.length() > 0) {
            builder.set("Host", hostHeader);
        }
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
        if (Objects.isNull(options.getRequestId())) {
            String requestId = UUID.randomUUID().toString();
            log.info("[ByteplusSDK] use requestId generated by sdk: '{}' ", requestId);
            builder.set("Request-Id", requestId);
        } else {
            builder.set("Request-Id", options.getRequestId());
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
        digest.update(getToken().getBytes(StandardCharsets.UTF_8));
        digest.update(httpBody);
        digest.update(getTenantID().getBytes(StandardCharsets.UTF_8));
        digest.update(ts.getBytes(StandardCharsets.UTF_8));
        digest.update(nonce.getBytes(StandardCharsets.UTF_8));

        return Helper.bytes2Hex(digest.digest());
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
        try {
            Response response = call.execute();
            ResponseBody rspBody = response.body();
            if (response.code() != SUCCESS_HTTP_CODE) {
                logHTTPResponse(url, response);
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
                log.error("[ByteplusSDK] do http request timeout, cost:{} msg:{} url:{}",
                        Duration.between(startTime, LocalDateTime.now()).toMillis(), e, url);
                throw new NetException(e.toString());
            }
            log.error("[ByteplusSDK] do http request occur exception, msg:{} url:{}", e, url);
            throw new BizException(e.toString());
        } finally {
            log.debug("[ByteplusSDK] http url:{}, cost:{}ms",
                    url, Duration.between(startTime, LocalDateTime.now()).toMillis());
        }
    }

    private Headers withAuthHeaders(Request request, byte[] bodyBytes) throws BizException {
        //air_auth
        if (isUseAirAuth()) {
            Headers originHeaders = request.headers();
            return withAirAuthHeaders(originHeaders, bodyBytes);
        }
        //volc_auth
        try {
            return VoclAuth.sign(request, bodyBytes, getCredential());
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
        String ts = "" + (System.currentTimeMillis() / 1000);
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
            // 二次检查，防止并发导致重复进入
            httpClient = timeoutHTTPCliMap.get(timeout);
            if (Objects.nonNull(httpClient)) {
                return httpClient;
            }
            httpClient = new OkHttpClient.Builder()
                    .callTimeout(timeout)
                    .build();
            // 使用ab替换，减少加锁操作
            Map<Duration, OkHttpClient> timeoutHTTPCliMapTemp = new HashMap<>(timeoutHTTPCliMap.size());
            timeoutHTTPCliMapTemp.putAll(timeoutHTTPCliMap);
            timeoutHTTPCliMapTemp.put(timeout, httpClient);
            timeoutHTTPCliMap = timeoutHTTPCliMapTemp;
            return httpClient;
        }
    }

    private void logHTTPResponse(String url, Response response) throws IOException {
        byte[] rspBody;
        if (Objects.isNull(response.body())) {
            log.error("[ByteplusSDK] http status not 200, url:{} code:{} msg:{} headers:\n{}",
                    url, response.code(), response.message(), response.headers());
            return;
        }
        String rspEncoding = response.header("Content-Encoding");
        if (Objects.isNull(rspEncoding) || !rspEncoding.contains("gzip")) {
            rspBody = response.body().bytes();
        } else {
            rspBody = gzipDecompress(response.body().bytes(), url);
        }
        log.error("[ByteplusSDK] http status not 200, url:{} code:{} msg:{} headers:\n{} body:\n{}",
                url, response.code(), response.message(), response.headers(), new String(rspBody));
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
}