package com.byteplus.rec.core;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public interface Option {
    void fill(Options options);

    static Options conv2Options(Option[] opts) {
        Options options = new Options();
        if (Objects.isNull(opts) || opts.length == 0) {
            return options;
        }
        for (Option opt : opts) {
            opt.fill(options);
        }
        return options;
    }

    // WithRequestID Specify the request_id manually. By default,
    // the SDK generates a unique request_id for each request using the UUID
    static Option withRequestID(String requestID) {
        return options -> options.setRequestID(requestID);
    }

    // Specifies the timeout for this request
    static Option withTimeout(Duration timeout) {
        return options -> {
            if (timeout.toMillis() <= 0) {
                return;
            }
            options.setTimeout(timeout);
        };
    }

    // WithHTTPHeader Add an HTTP header to the request.
    // In general, you do not need to care this.
    static Option withHTTPHeader(String key, String value) {
        return options -> {
            Map<String, String> headers = options.getHeaders();
            if (headers == null) {
                headers = new HashMap<>();
            }
            headers.put(key, value);
            options.setHeaders(headers);
        };
    }

    // WithHTTPQuery Add an HTTP query to the request.
    // In general, you do not need to care this.
    static Option withHTTPQuery(String key, String value) {
        return options -> {
            Map<String, String> queries = options.getQueries();
            if (queries == null) {
                queries = new HashMap<>();
            }
            queries.put(key, value);
            options.setQueries(queries);
        };
    }

    // WithServerTimeout Specifies the maximum time it will take for
    // the server to process the request. The server will try to return
    // the result within this time, even if the task is not completed
    static Option withServerTimeout(Duration timeout) {
        return options -> options.setServerTimeout(timeout);
    }
}

