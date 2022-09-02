package com.byteplus.rec.core;

import java.time.Duration;

public final class Constant {
    public final static int HTTP_STATUS_OK = 200;

    public final static int HTTP_STATUS_NOT_FOUND = 404;

    /**
     * All requests will have a XXXResponse corresponding to them,
     * and all XXXResponses will contain a 'Status' field.
     * The status of this request can be determined by the value of `Status.Code`
     * Detail error code info：https://docs.byteplus.com/docs/error-code
     */
    // The request was executed successfully without any exception
    public final static int STATUS_CODE_SUCCESS = 0;

    // A Request with the same "Request-ID" was already received. This Request was rejected
    public final static int STATUS_CODE_IDEMPOTENT = 409;

    // Operation information is missing due to an unknown exception
    public final static int STATUS_CODE_OPERATION_LOSS = 410;

    // The server hope slow down request frequency, and this request was rejected
    public final static int STATUS_CODE_TOO_MANY_REQUEST = 429;

    // The default keepalive duration
    public final static Duration DEFAULT_KEEPALIVE_DURATION = Duration.ofSeconds(60);

    // The default keepalive ping interval
    public final static Duration DEFAULT_KEEPALIVE_PING_INTERVAL = Duration.ofSeconds(45);

    // The default max idle connections of okhttp client connection pool
    public final static int DEFAULT_MAX_IDLE_CONNECTIONS = 32;

    // Metrics Key
    public final static String METRICS_KEY_COMMON_INFO = "common.info";
    public final static String METRICS_KEY_COMMON_WARN = "common.warn";
    public final static String METRICS_KEY_COMMON_ERROR = "common.err";
    public final static String METRICS_KEY_REQUEST_TOTAL_COST = "request.total.cost";
    public final static String METRICS_KEY_REQUEST_COST = "request.cost";
    public final static String METRICS_KEY_REQUEST_COUNT = "request.count";
    public final static String METRICS_KEY_NETWORK_DNS_START = "network.dns.start";
    public final static String METRICS_KEY_NETWORK_CONNECT_START = "network.connect.start";
    public final static String METRICS_KEY_NETWORK_SECURE_CONNECT_START = "network.secure.connect.start";
    public final static String METRICS_KEY_NETWORK_CONNECT_FAIL = "network.connect.fail";
    public final static String METRICS_KEY_NETWORK_CALL_FAIL = "network.call.fail";
    public final static String METRICS_KEY_HEARTBEAT_COUNT = "heartbeat.count";
}
