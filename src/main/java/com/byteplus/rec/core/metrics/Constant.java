package com.byteplus.rec.core.metrics;


public class Constant {
    // metrics default domain and prefix
    public static final String DEFAULT_METRICS_DOMAIN = "rec-api-sg1.recplusapi.com";
    public static final String DEFAULT_METRICS_PREFIX = "byteplus.rec.sdk";
    public static final String DEFAULT_METRICS_HTTP_SCHEMA = "https";

    // monitor url format
    public static final String METRICS_URL_FORMAT = "%s://%s/predict/api/monitor/metrics";
    public static final String METRICS_LOG_URL_FORMAT = "%s://%s/predict/api/monitor/metrics/log";

    // domain path
    public static final String METRICS_PATH = "/monitor/metrics";
    public static final String METRICS_LOG_PATH = "/monitor/metrics/log";

    // metrics flush interval
    public static final int DEFAULT_FLUSH_INTERVAL_MS = 15 * 1000;

    public final static int DEFAULT_HTTP_TIMEOUT_MS = 800;

    public final static int MAX_TRY_TIMES = 3;

    public final static int MAX_SPIN_TIMES = 5;

    public final static int SUCCESS_HTTP_CODE = 200;

    // metrics log level
    public final static String LOG_LEVEL_TRACE = "trace";
    public final static String LOG_LEVEL_DEBUG = "debug";
    public final static String LOG_LEVEL_INFO = "info";
    public final static String LOG_LEVEL_NOTICE = "notice";
    public final static String LOG_LEVEL_WARN = "warn";
    public final static String LOG_LEVEL_ERROR = "error";
    public final static String LOG_LEVEL_FATAL = "fatal";

    // metrics type
    public final static String METRICS_TYPE_COUNTER = "counter";
    public final static String METRICS_TYPE_STORE = "store";
    public final static String METRICS_TYPE_TIMER = "timer";
    public final static String METRICS_TYPE_RATE_COUNTER = "rate_counter";
    public final static String METRICS_TYPE_METER = "meter";
}
