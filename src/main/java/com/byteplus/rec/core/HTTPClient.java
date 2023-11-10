package com.byteplus.rec.core;

import com.byteplus.rec.core.metrics.MetricsCollector;
import com.byteplus.rec.core.metrics.MetricsCollector.MetricsCfg;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import okhttp3.OkHttpClient;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class HTTPClient {
    private HTTPCaller httpCaller;
    private HostAvailabler hostAvailabler;
    private String schema;

    public <Rsp extends Message, Req extends Message> Rsp doPBRequest(
            String path,
            Req request,
            Parser<Rsp> rspParser,
            Options options) throws NetException, BizException {
        return httpCaller.doPBRequest(buildURL(path), request, rspParser, options);
    }

    public <Rsp> Rsp doJSONRequest(
            String path,
            Object request,
            Rsp response,
            Options options) throws NetException, BizException {
        return httpCaller.doJSONRequest(buildURL(path), request, response, options);
    }

    private String buildURL(String path) {
        String host = hostAvailabler.getHost(path);
        return Utils.buildURL(schema, host, path);
    }

    public void shutdown() {
        hostAvailabler.shutdown();
        httpCaller.shutdown();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    public static class Builder {
        private static HostAvailabler globalHostAvailabler;

        private String tenantID;

        private String projectID;

        private boolean useAirAuth;

        private String airAuthToken;

        private String authAK;

        private String authSK;

        private String authService;

        private String schema;

        private List<String> hosts;

        private String mainHost;

        private IRegion region;

        private HostAvailablerFactory hostAvailablerFactory;

        private boolean keepAlive;

        private HTTPCaller.Config callerConfig;

        private HostAvailabler hostAvailabler;

        private MetricsCfg metricsCfg;

        @Deprecated
        // If you want to customize the OKHTTPClient, you can pass in this parameter,
        // and all subsequent requests from the client will use this incoming OKHTTPClient.
        private OkHttpClient callerClient;

        public HTTPClient build() throws BizException {
            checkRequiredField();
            fillDefault();
            // init only use metrics
            if (!MetricsCollector.isInitialed() && Objects.nonNull(metricsCfg)) {
                if (metricsCfg.isEnableMetrics() || metricsCfg.isEnableMetricsLog()) {
                    initGlobalHostAvailabler();
                }
            }
            MetricsCollector.Init(metricsCfg, globalHostAvailabler);
            return new HTTPClient(newHTTPCaller(), hostAvailabler, schema);
        }

        private void checkRequiredField() throws BizException {
            if (Objects.isNull(tenantID)) {
                throw new BizException("tenant id is null");
            }
            checkAuthRequiredField();
            if (Objects.isNull(region)) {
                throw new BizException("region is null");
            }
        }

        private void checkAuthRequiredField() throws BizException {
            // air auth need token
            if (useAirAuth) {
                if (Objects.isNull(airAuthToken) || airAuthToken.equals("")) {
                    throw new BizException("token cannot be null");
                }
                return;
            }
            // auth need ak and sk
            if (Objects.isNull(authAK) || authAK.equals("") ||
                    Objects.isNull(authSK) || authSK.equals("")) {
                throw new BizException("ak and sk cannot be null");
            }
        }

        private void fillDefault() throws BizException {
            if (Objects.isNull(schema) || schema.equals("")) {
                schema = "https";
            }
            // fill hostAvailabler Factory.
            if (Objects.isNull(hostAvailablerFactory)) {
                hostAvailablerFactory = new HostAvailablerFactory();
            }
            hostAvailabler = newHostAvailabler();
            // fill default caller config.
            if (Objects.isNull(callerConfig)) {
                callerConfig = HTTPCaller.getDefaultConfig();
            }
        }

        private synchronized void initGlobalHostAvailabler() throws BizException {
            if (Objects.nonNull(globalHostAvailabler)) {
                return;
            }
            globalHostAvailabler = newHostAvailabler();
        }

        private HostAvailabler newHostAvailabler() throws BizException {
            // if '.hosts' is set, then skip fetch hosts from server
            if (Utils.isNotEmptyList(hosts)) {
                return hostAvailablerFactory.newHostAvailabler(projectID, hosts, mainHost, true);
            }
            return hostAvailablerFactory.newHostAvailabler(projectID, region.getHosts(), mainHost, false);
        }

        private HTTPCaller newHTTPCaller() {
            if (useAirAuth) {
                return new HTTPCaller(projectID, tenantID, airAuthToken, hostAvailabler, callerConfig, schema, keepAlive);
            }
            String authRegion = region.getAuthRegion();
            Auth.Credential credential = new Auth.Credential(authAK, authSK, authService, authRegion);
            return new HTTPCaller(projectID, tenantID, credential, hostAvailabler, callerConfig, schema, keepAlive);
        }
    }
}
