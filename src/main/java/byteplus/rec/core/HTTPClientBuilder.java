package byteplus.rec.core;

import byteplus.rec.core.VoclAuth.Credential;
import byteplus.rec.core.PingHostAvailabler;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Objects;

@Accessors(fluent = true, chain = true)
@Setter
public class HTTPClientBuilder {
    private String tenantID;

    private String token;

    private String ak;

    private String sk;

    private String authService;

    private boolean useAirAuth;

    private String schema;

    private List<String> hosts;

    private String hostHeader;

    private IRegion region;

    private HostAvailabler hostAvailabler;

    public HTTPClient build() {
        checkRequiredField();
        fillHosts();
        fillDefault();
        return new HTTPClient(newHTTPCaller(), hostAvailabler, schema);
    }

    private void checkRequiredField() {
        if (Objects.isNull(tenantID)) {
            throw new RuntimeException("Tenant id is null");
        }
        checkAuthRequiredField();
        if (Objects.isNull(region)) {
            throw new RuntimeException("Region is null");
        }
    }

    private void checkAuthRequiredField() {
        // air auth need token
        if (useAirAuth) {
            if (Objects.isNull(token) || token.equals("")) {
                throw new RuntimeException("token cannot be null");
            }
            return;
        }
        // volc auth need ak and sk
        if (Objects.isNull(ak) || ak.equals("") ||
                Objects.isNull(sk) || sk.equals("")) {
            throw new RuntimeException("ak and sk cannot be null");
        }
    }

    private void fillHosts() {
        if (Objects.nonNull(hosts) && !hosts.isEmpty()) {
            return;
        }
        hosts = region.getHosts();
    }

    private void fillDefault() {
        if (Objects.isNull(schema) || schema.equals("")) {
            schema = "https";
        }
        if (hostAvailabler == null) {
            PingHostAvailabler.Config config = new PingHostAvailabler.Config(hosts, hostHeader);
            hostAvailabler = new PingHostAvailabler(config);
        }
        if (Objects.isNull(hostAvailabler.hosts()) || hostAvailabler.hosts().isEmpty()) {
            hostAvailabler.setHosts(hosts);
        }
        if (Objects.isNull(hostAvailabler.hostHeader()) || hostAvailabler.hostHeader().length() == 0) {
            hostAvailabler.setHostHeader(hostHeader);
        }
    }

    private HTTPCaller newHTTPCaller() {
        String volcCredentialRegion = region.getVolcCredentialRegion();
        Credential cred = new Credential(ak, sk, authService, volcCredentialRegion);
        return new HTTPCaller(tenantID, token, useAirAuth, hostHeader, cred);
    }
}
