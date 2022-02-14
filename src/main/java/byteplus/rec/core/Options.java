package byteplus.rec.core;

import lombok.Data;

import java.time.Duration;
import java.util.Map;

@Data
public class Options {
    private Duration timeout;

    private String RequestId;

    private Map<String, String> headers;

    private Map<String, String> queries;

    private Duration serverTimeout;
}
