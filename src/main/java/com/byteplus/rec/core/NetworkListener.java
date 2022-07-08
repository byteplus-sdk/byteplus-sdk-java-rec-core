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
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, callStart={}, url={}",
                call.request().headers("Request-Id"), System.currentTimeMillis(), call.request().url());
    }

    @Override
    public void dnsStart(@NotNull Call call, @NotNull String domainName) {
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, dnsStart={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void dnsEnd(@NotNull Call call, @NotNull String domainName, @NotNull List<InetAddress> inetAddressList) {
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, dnsEnd={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void connectStart(@NotNull Call call, @NotNull InetSocketAddress inetSocketAddress, @NotNull Proxy proxy) {
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, connectStart={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void secureConnectStart(@NotNull Call call) {
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, secureConnectStart={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void secureConnectEnd(@NotNull Call call, @Nullable Handshake handshake) {
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, secureConnectEnd={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void connectEnd(@NotNull Call call, @NotNull InetSocketAddress inetSocketAddress,
                           @NotNull Proxy proxy, @Nullable Protocol protocol) {
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, connectEnd={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void connectFailed(@NotNull Call call, @NotNull InetSocketAddress inetSocketAddress, @NotNull Proxy proxy, @Nullable Protocol protocol, @NotNull IOException ioe) {
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, connectFailed={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void requestHeadersStart(@NotNull Call call) {
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, requestHeadersStart={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void requestHeadersEnd(@NotNull Call call, @NotNull Request request) {
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, requestHeadersEnd={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void requestBodyStart(@NotNull Call call) {
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, requestBodyStart={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void requestBodyEnd(@NotNull Call call, long byteCount) {
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, requestBodyEnd={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void responseHeadersStart(@NotNull Call call) {
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, responseHeadersStart={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void responseHeadersEnd(@NotNull Call call, @NotNull Response response) {
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, responseHeadersEnd={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void responseBodyStart(@NotNull Call call) {
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, responseBodyStart={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void responseBodyEnd(@NotNull Call call, long byteCount) {
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, responseBodyEnd={}", call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void callEnd(@NotNull Call call) {
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, callEnd={}, url={}", call.request().headers("Request-Id"), System.currentTimeMillis(), call.request().url());
    }

    @Override
    public void callFailed(@NotNull Call call, @NotNull IOException ioe) {
        log.debug("[ByteplusSDK][NetworkListener]: request id={}, callFailed={}, err={}", call.request().headers("Request-Id"), System.currentTimeMillis(), ioe.getMessage());
    }

}
