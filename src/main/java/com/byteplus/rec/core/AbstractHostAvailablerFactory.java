package com.byteplus.rec.core;

import java.util.List;

// Implement custom HostAvailabler by overriding AbstractHostAvailablerFactory.
public class AbstractHostAvailablerFactory {
    public HostAvailabler newHostAvailabler(String projectID, List<String> hosts) throws BizException {
        return new PingHostAvailabler(projectID, hosts);
    }

    public HostAvailabler newHostAvailabler(List<String> hosts) throws BizException {
        return new PingHostAvailabler(hosts);
    }
}
