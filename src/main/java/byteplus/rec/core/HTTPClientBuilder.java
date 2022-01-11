package byteplus.rec.core;

import byteplus.rec.core.volcAuth.Credential;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Objects;

@Accessors(chain = true)
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

    private String region;

    private HostAvailabler hostAvailabler;

    public HTTPClient build() {
        checkRequiredField();
        fillHosts();
        fillDefault();
        Credential credential = buildVolcCredential();
        HTTPCaller httpCaller = new HTTPCaller(tenantID, token, useAirAuth, credential);
        return new HTTPClient(httpCaller, hostAvailabler, schema);
    }

    private void checkRequiredField() {
        if (Objects.isNull(tenantID)) {
            throw new RuntimeException("Tenant id is null");
        }
        checkAuthRequiredField();
        if (Objects.isNull(region) || region.equals(RegionHelper.REGION_UNKNOWN)) {
            throw new RuntimeException("Region is null");
        }
        if (Objects.isNull(RegionHelper.getRegionConfig(region))) {
            throw new RuntimeException(String.format("region(%s) is not support", region));
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
        hosts = RegionHelper.getRegionHosts(region);
    }

    private void fillDefault() {
        if (Objects.isNull(schema) || schema.equals("")) {
            schema = "https";
        }
        if (hostAvailabler == null) {
            PingHostAvailabler.PingHostAvailablerConfig config = new PingHostAvailabler.PingHostAvailablerConfig(hosts);
            hostAvailabler = new PingHostAvailabler(config);
        }
        if (Objects.isNull(hostAvailabler.Hosts()) || hostAvailabler.Hosts().isEmpty()) {
            hostAvailabler.setHosts(hosts);
        }
    }

    private Credential buildVolcCredential() {
        String volcCredentialRegion = RegionHelper.getVolcCredentialRegion(region);
        return new Credential(ak, sk, authService, volcCredentialRegion);
    }
}
