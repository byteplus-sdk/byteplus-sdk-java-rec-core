package com.byteplus.rec.core;

import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.List;


@Slf4j
public class NetworkListener extends EventListener {
    public static Factory get() {
        return new Factory() {
            @NotNull
            @Override
            public EventListener create(@NotNull Call call) {
                return new NetworkListener();
            }
        };
    }

    @Override
    public void callStart(@NotNull Call call) {
        super.callStart(call);
        log.debug("request id={}, callStart={}, url={}",
                call.request().headers("Request-Id"), System.currentTimeMillis(), call.request().url());
    }

    @Override
    public void dnsStart(@NotNull Call call, @NotNull String domainName) {
        super.dnsStart(call, domainName);
        log.debug("request id={}, dnsStart={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void dnsEnd(@NotNull Call call, @NotNull String domainName, @NotNull List<InetAddress> inetAddressList) {
        super.dnsEnd(call, domainName, inetAddressList);
        log.debug("request id={}, dnsEnd={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void connectStart(@NotNull Call call, @NotNull InetSocketAddress inetSocketAddress, @NotNull Proxy proxy) {
        super.connectStart(call, inetSocketAddress, proxy);
        log.debug("request id={}, connectStart={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void secureConnectStart(@NotNull Call call) {
        super.secureConnectStart(call);
        log.debug("request id={}, secureConnectStart={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void secureConnectEnd(@NotNull Call call, @Nullable Handshake handshake) {
        super.secureConnectEnd(call, handshake);
        log.debug("request id={}, secureConnectEnd={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void connectEnd(@NotNull Call call, @NotNull InetSocketAddress inetSocketAddress,
                           @NotNull Proxy proxy, @Nullable Protocol protocol) {
        super.connectEnd(call, inetSocketAddress, proxy, protocol);
        log.debug("request id={}, connectEnd={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void connectFailed(@NotNull Call call, @NotNull InetSocketAddress inetSocketAddress, @NotNull Proxy proxy, @Nullable Protocol protocol, @NotNull IOException ioe) {
        super.connectFailed(call, inetSocketAddress, proxy, protocol, ioe);
        log.debug("request id={}, connectFailed={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void requestHeadersStart(@NotNull Call call) {
        super.requestHeadersStart(call);
        log.debug("request id={}, requestHeadersStart={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void requestHeadersEnd(@NotNull Call call, @NotNull Request request) {
        super.requestHeadersEnd(call, request);
        log.debug("request id={}, requestHeadersEnd={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void requestBodyStart(@NotNull Call call) {
        super.requestBodyStart(call);
        log.debug("request id={}, requestBodyStart={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void requestBodyEnd(@NotNull Call call, long byteCount) {
        super.requestBodyEnd(call, byteCount);
        log.debug("request id={}, requestBodyEnd={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void responseHeadersStart(@NotNull Call call) {
        super.responseHeadersStart(call);
        log.debug("request id={}, responseHeadersStart={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void responseHeadersEnd(@NotNull Call call, @NotNull Response response) {
        super.responseHeadersEnd(call, response);
        log.debug("request id={}, responseHeadersEnd={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void responseBodyStart(@NotNull Call call) {
        super.responseBodyStart(call);
        log.debug("request id={}, responseBodyStart={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void responseBodyEnd(@NotNull Call call, long byteCount) {
        super.responseBodyEnd(call, byteCount);
        log.debug("request id={}, responseBodyEnd={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void callEnd(@NotNull Call call) {
        super.callEnd(call);
        log.debug("request id={}, callEnd={}, url={}", call.request().headers("Request-Id"), System.currentTimeMillis(),call.request().url());
    }

    @Override
    public void callFailed(@NotNull Call call, @NotNull IOException ioe) {
        super.callFailed(call, ioe);
        log.debug("request id={}, callFailed={}, err={}", call.request().headers("Request-Id"), System.currentTimeMillis(), ioe.getMessage());
    }

}
