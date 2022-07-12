package com.byteplus.rec.core;

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

    @Override
    public void callStart(@NotNull Call call) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, callStart={}, url={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis(), call.request().url());
    }

    @Override
    public void dnsStart(@NotNull Call call, @NotNull String domainName) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, dnsStart={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void dnsEnd(@NotNull Call call, @NotNull String domainName, @NotNull List<InetAddress> inetAddressList) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, dnsEnd={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void connectStart(@NotNull Call call, @NotNull InetSocketAddress inetSocketAddress, @NotNull Proxy proxy) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, connectStart={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void secureConnectStart(@NotNull Call call) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, secureConnectStart={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void secureConnectEnd(@NotNull Call call, @Nullable Handshake handshake) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, secureConnectEnd={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void connectEnd(@NotNull Call call, @NotNull InetSocketAddress inetSocketAddress,
                           @NotNull Proxy proxy, @Nullable Protocol protocol) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, connectEnd={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void connectFailed(@NotNull Call call, @NotNull InetSocketAddress inetSocketAddress, @NotNull Proxy proxy, @Nullable Protocol protocol, @NotNull IOException ioe) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, connectFailed={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void requestHeadersStart(@NotNull Call call) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, requestHeadersStart={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void requestHeadersEnd(@NotNull Call call, @NotNull Request request) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, requestHeadersEnd={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void requestBodyStart(@NotNull Call call) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, requestBodyStart={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void requestBodyEnd(@NotNull Call call, long byteCount) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, requestBodyEnd={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void responseHeadersStart(@NotNull Call call) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, responseHeadersStart={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void responseHeadersEnd(@NotNull Call call, @NotNull Response response) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, responseHeadersEnd={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void responseBodyStart(@NotNull Call call) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, responseBodyStart={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void responseBodyEnd(@NotNull Call call, long byteCount) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, responseBodyEnd={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis());
    }

    @Override
    public void callEnd(@NotNull Call call) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, callEnd={}, url={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis(), call.request().url());
    }

    @Override
    public void callFailed(@NotNull Call call, @NotNull IOException ioe) {
        log.info("[ByteplusSDK][NetworkListener] host={}, request id={}, callFailed={}, err={}",
                addr, call.request().headers("Request-Id"), System.currentTimeMillis(), ioe.getMessage());
    }

}
