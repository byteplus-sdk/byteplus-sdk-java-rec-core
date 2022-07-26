package com.byteplus.rec.core;

import com.byteplus.rec.core.metrics.Metrics;
import com.byteplus.rec.core.metrics.MetricsLog;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.UnknownHostException;
import java.util.List;


@Slf4j
public class NetworkListener extends EventListener {
    static InetAddress addr;

    static {
        try {
            addr = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public static Factory get() {
        return new Factory() {
            @NotNull
            @Override
            public EventListener create(@NotNull Call call) {
                return new NetworkListener();
            }
        };
    }

    public String getReqID(@NotNull Call call) {
        return call.request().headers("Request-Id").get(0);
    }

    public String getProjectID(@NotNull Call call) {
        return call.request().headers("Project-Id").get(0);
    }

    @Override
    public void callStart(@NotNull Call call) {
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, callStart=%d, url=%s",
                getProjectID(call), addr, currentTimestamp, call.request().url());
    }

    @Override
    public void dnsStart(@NotNull Call call, @NotNull String domainName) {
        String[] metricsTags = new String[]{
                "url:" + Utils.escapeMetricsTagValue(call.request().url().toString()),
                "project_id:" + getProjectID(call)
        };
        Metrics.counter(Constant.METRICS_KEY_NETWORK_DNS_START, 1, metricsTags);
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, dnsStart=%d",
                getProjectID(call), addr, currentTimestamp);
    }

    @Override
    public void dnsEnd(@NotNull Call call, @NotNull String domainName, @NotNull List<InetAddress> inetAddressList) {
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, dnsEnd=%d",
                getProjectID(call), addr, currentTimestamp);
    }

    @Override
    public void connectStart(@NotNull Call call, @NotNull InetSocketAddress inetSocketAddress, @NotNull Proxy proxy) {
        String[] metricsTags = new String[]{
                "url:" + Utils.escapeMetricsTagValue(call.request().url().toString()),
                "project_id:" + getProjectID(call)
        };
        Metrics.counter(Constant.METRICS_KEY_NETWORK_CONNECT_START, 1, metricsTags);
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, connectStart=%d",
                getProjectID(call), addr, currentTimestamp);
    }

    @Override
    public void secureConnectStart(@NotNull Call call) {
        String[] metricsTags = new String[]{
                "url:" + Utils.escapeMetricsTagValue(call.request().url().toString()),
                "project_id:" + getProjectID(call)
        };
        Metrics.counter(Constant.METRICS_KEY_NETWORK_SECURE_CONNECT_START, 1, metricsTags);
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, secureConnectStart=%d",
                getProjectID(call), addr, currentTimestamp);
    }

    @Override
    public void secureConnectEnd(@NotNull Call call, @Nullable Handshake handshake) {
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, secureConnectEnd=%d",
                getProjectID(call), addr, currentTimestamp);
    }

    @Override
    public void connectEnd(@NotNull Call call, @NotNull InetSocketAddress inetSocketAddress,
                           @NotNull Proxy proxy, @Nullable Protocol protocol) {
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, connectEnd=%d",
                getProjectID(call), addr, currentTimestamp);
    }

    @Override
    public void connectFailed(@NotNull Call call, @NotNull InetSocketAddress inetSocketAddress, @NotNull Proxy proxy, @Nullable Protocol protocol, @NotNull IOException ioe) {
        String[] metricsTags = new String[]{
                "url:" + Utils.escapeMetricsTagValue(call.request().url().toString()),
                "project_id:" + getProjectID(call)
        };
        Metrics.counter(Constant.METRICS_KEY_NETWORK_CONNECT_FAIL, 1, metricsTags);
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, connectFailed=%d",
                getProjectID(call), addr, currentTimestamp);
    }

    @Override
    public void requestHeadersStart(@NotNull Call call) {
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, requestHeadersStart=%d",
                getProjectID(call), addr, currentTimestamp);
    }

    @Override
    public void requestHeadersEnd(@NotNull Call call, @NotNull Request request) {
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, requestHeadersEnd=%d",
                getProjectID(call), addr, currentTimestamp);
    }

    @Override
    public void requestBodyStart(@NotNull Call call) {
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, requestBodyStart=%d",
                getProjectID(call), addr, currentTimestamp);
    }

    @Override
    public void requestBodyEnd(@NotNull Call call, long byteCount) {
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, requestBodyEnd=%d",
                getProjectID(call), addr, currentTimestamp);
    }

    @Override
    public void responseHeadersStart(@NotNull Call call) {
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, responseHeadersStart=%d",
                getProjectID(call), addr, currentTimestamp);
    }

    @Override
    public void responseHeadersEnd(@NotNull Call call, @NotNull Response response) {
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, responseHeadersEnd=%d",
                getProjectID(call), addr, currentTimestamp);
    }

    @Override
    public void responseBodyStart(@NotNull Call call) {
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, responseBodyStart=%d",
                getProjectID(call), addr, currentTimestamp);
    }

    @Override
    public void responseBodyEnd(@NotNull Call call, long byteCount) {
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, responseBodyEnd=%d",
                getProjectID(call), addr, currentTimestamp);
    }

    @Override
    public void callEnd(@NotNull Call call) {
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, callEnd=%d, url=%s",
                getProjectID(call), addr, currentTimestamp, call.request().url());
    }

    @Override
    public void callFailed(@NotNull Call call, @NotNull IOException ioe) {
        String[] metricsTags = new String[]{
                "url:" + Utils.escapeMetricsTagValue(call.request().url().toString()),
                "project_id:" + getProjectID(call)
        };
        Metrics.counter(Constant.METRICS_KEY_NETWORK_CALL_FAIL, 1, metricsTags);
        long currentTimestamp = System.currentTimeMillis();
        MetricsLog.info(getReqID(call), "[ByteplusSDK][NetworkListener] project_id=%s, host=%s, callFailed=%d, err=%s",
                getProjectID(call), addr, currentTimestamp, ioe.getMessage());
    }

}
