package com.byteplus.rec.core;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

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

    public static OkHttpClient buildOkHTTPClient(Duration timeout) {
        return new OkHttpClient.Builder()
                .connectTimeout(timeout)
                .writeTimeout(timeout)
                .readTimeout(timeout)
                .callTimeout(timeout)
                .build();
    }

    public static boolean ping(OkHttpClient httpCli, String pingURLFormat, String host) {
        String url = String.format(pingURLFormat, host);
        Headers.Builder builder = new Headers.Builder();
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
            if (isPingSuccess(httpRsp)) {
                log.debug("[ByteplusSDK] ping success, host:{} cost:{}ms", host, cost);
                return true;
            }
            log.warn("[ByteplusSDK] ping fail, host:{} cost:{}ms status:{}", host, cost, httpRsp.code());
            return false;
        } catch (Throwable e) {
            long cost = clock.millis() - start;
            log.warn("[ByteplusSDK] ping find err, host:'{}' cost:{}ms err:'{}'", host, cost, e.toString());
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
