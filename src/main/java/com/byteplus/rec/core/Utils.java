package com.byteplus.rec.core;

import com.byteplus.rec.core.metrics.Metrics;
import com.byteplus.rec.core.metrics.MetricsCollector;
import com.byteplus.rec.core.metrics.MetricsLog;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Utils {
    private final static Clock clock = Clock.systemDefaultZone();

    public interface Callable<Rsp extends Message, Req> {
        Rsp call(Req req, Option... opts) throws BizException, NetException;
    }

    public static <Rsp extends Message, Req> Rsp doWithRetry(
            Callable<Rsp, Req> callable,
            Req req,
            Option[] opts,
            int retryTimes) throws BizException {

        Rsp rsp = null;
        int tryTimes = retryTimes < 0 ? 1 : retryTimes + 1;
        for (int i = 0; i < tryTimes; i++) {
            try {
                rsp = callable.call(req, opts);
            } catch (NetException e) {
                if (i == tryTimes - 1) {
                    log.error("[DoRetryRequest] fail finally after retried {} times", tryTimes);
                    throw new BizException(e.getMessage());
                }
                continue;
            }
            break;
        }
        return rsp;
    }

    public static String bytes2Hex(byte[] bts) {
        StringBuilder sb = new StringBuilder();
        String hex;
        for (byte bt : bts) {
            hex = (Integer.toHexString(bt & 0xff));
            if (hex.length() == 1) {
                sb.append("0");
            }
            sb.append(hex);
        }
        return sb.toString();
    }

    public static String buildURL(String schema, String host, String path) {
        if (path.charAt(0) == '/') {
            return String.format("%s://%s%s", schema, host, path);
        }
        return String.format("%s://%s/%s", schema, host, path);
    }

    public static OkHttpClient buildOkHTTPClient(Duration timeout, int maxIdleConnections,
                                                 Duration keepAliveDuration) {
        OkHttpClient client = new OkHttpClient.Builder()
                .connectionPool(new ConnectionPool(
                        maxIdleConnections,
                        keepAliveDuration.toMillis(),
                        TimeUnit.MILLISECONDS)
                )
                // Has no practical effect, only used for websocket
                .pingInterval(Constant.DEFAULT_KEEPALIVE_PING_INTERVAL)
                .build();
        return buildOkHTTPClient(client, timeout);
    }

    public static OkHttpClient buildOkHTTPClient(Duration timeout) {
        OkHttpClient client = new OkHttpClient.Builder()
                .connectionPool(new ConnectionPool(
                        Constant.DEFAULT_MAX_IDLE_CONNECTIONS,
                        Constant.DEFAULT_KEEPALIVE_DURATION.toMillis(),
                        TimeUnit.MILLISECONDS)
                )
                .pingInterval(Constant.DEFAULT_KEEPALIVE_PING_INTERVAL)
                .build();
        return buildOkHTTPClient(client, timeout);
    }

    public static OkHttpClient buildOkHTTPClient(OkHttpClient client, Duration timeout) {
        return doBuild(client.newBuilder(), timeout);
    }

    private static OkHttpClient doBuild(OkHttpClient.Builder okHTTPBuilder, Duration timeout) {
        okHTTPBuilder
                .connectTimeout(timeout)
                .writeTimeout(timeout)
                .readTimeout(timeout)
                .callTimeout(timeout);
        if (MetricsCollector.isEnableMetricsLog() || MetricsCollector.isEnableMetrics()) {
            okHTTPBuilder.eventListenerFactory(NetworkListener.get());
        }
        return okHTTPBuilder.build();
    }

    public static boolean ping(String projectID, OkHttpClient httpCli, String pingURLFormat,
                               String schema, String host) {
        String url = String.format(pingURLFormat, schema, host);
        Headers.Builder builder = new Headers.Builder();
        String reqID = "ping_" + UUID.randomUUID();
        builder.set("Request-Id", reqID);
        if (Objects.nonNull(projectID)) {
            builder.set("Project-Id", projectID);
        }
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
            MetricsLog.info(reqID, "[ByteplusSDK][ping] project_id:%s, sent: %d, received: %d, cost:%d, connection count:%d",
                    projectID, httpRsp.sentRequestAtMillis(), httpRsp.receivedResponseAtMillis(),
                    cost, httpCli.connectionPool().connectionCount());
            log.debug("[ByteplusSDK][ping] sent: {}, received: {}, cost:{}, connection count:{}",
                    httpRsp.sentRequestAtMillis(), httpRsp.receivedResponseAtMillis(),
                    cost, httpCli.connectionPool().connectionCount());
            if (isPingSuccess(httpRsp)) {
                MetricsLog.info(reqID, "[ByteplusSDK] ping success, project_id:%s, host:%s, cost:%dms",
                        projectID, Utils.escapeMetricsTagValue(host), cost);
                log.debug("[ByteplusSDK] ping success, host:{} cost:{}ms", host, cost);
                return true;
            }
            MetricsLog.warn(reqID, "[ByteplusSDK] ping fail, project_id:%s, host:%s, cost:%dms, status:%d",
                    projectID, Utils.escapeMetricsTagValue(host), cost, httpRsp.code());
            log.warn("[ByteplusSDK] ping fail, host:{} cost:{}ms status:{}", host, cost, httpRsp.code());
            return false;
        } catch (Throwable e) {
            long cost = clock.millis() - start;
            MetricsLog.warn(reqID, "[ByteplusSDK] ping find err, project_id:%s, host:%s, cost:%dms, err:%s",
                    projectID, Utils.escapeMetricsTagValue(host), cost, e.getMessage());
            log.warn("[ByteplusSDK] ping find err, host:'{}' cost:{}ms err:'{}'", host, cost, e.getMessage());
            return false;
        }
    }

    private static boolean isPingSuccess(Response httpRsp) throws IOException {
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

    // The recommended platform only supports the following strings.
    // ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.-_/:
    // If there are ?, & and = in the query, replace them all
    public static String escapeMetricsTagValue(String value) {
        value = value.replace("?", "-qu-");
        value = value.replace("&", "-and-");
        value = value.replace("=", "-eq-");
        return value;
    }

    public static boolean noneEmptyString(String... str) {
        if (Objects.isNull(str)) {
            return false;
        }
        for (String s : str) {
            if (Objects.isNull(s) || s.length() == 0) {
                return false;
            }
        }
        return true;
    }

    public static boolean isAllEmptyString(String... str) {
        if (Objects.isNull(str)) {
            return true;
        }
        for (String s : str) {
            if (Objects.nonNull(s) && s.length() > 0) {
                return false;
            }
        }
        return true;
    }

    public static boolean isEmptyString(String str) {
        return Objects.isNull(str) || str.length() == 0;
    }

    public static boolean isEmptyList(List<?> list) {
        return Objects.isNull(list) || list.size() == 0;
    }

    public static boolean isNotEmptyList(List<?> list) {
        return !isEmptyList(list);
    }
}
