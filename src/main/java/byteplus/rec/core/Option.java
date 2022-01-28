package byteplus.rec.core;

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

    static Option withTimeout(Duration timeout) {
        return options -> {
            if (timeout.toMillis() <= 0) {
                return;
            }
            options.setTimeout(timeout);
        };
    }

    static Option withRequestId(String requestId) {
        return options -> options.setRequestId(requestId);
    }

    static Option withHeaders(Map<String, String> headers) {
        return options -> options.setHeaders(headers);
    }

    static Option withHeader(String key, String value) {
        return options -> {
            Map<String, String> headers = options.getHeaders();
            if (headers == null) {
                headers = new HashMap<>();
            }
            headers.put(key, value);
            options.setHeaders(headers);
        };
    }

    static Option withQueries(Map<String, String> queries) {
        return options -> options.setQueries(queries);
    }

    static Option withQuery(String key, String value) {
        return options -> {
            Map<String, String> queries = options.getQueries();
            if (queries == null) {
                queries = new HashMap<>();
            }
            queries.put(key, value);
            options.setQueries(queries);
        };
    }

    static Option withServerTimeout(Duration timeout) {
        return options -> options.setServerTimeout(timeout);
    }
}

