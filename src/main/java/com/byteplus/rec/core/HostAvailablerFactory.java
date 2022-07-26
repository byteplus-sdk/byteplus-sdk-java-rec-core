package com.byteplus.rec.core;

import java.util.List;

// Implement custom HostAvailabler by overriding HostAvailablerFactory.
public class HostAvailablerFactory {
    public HostAvailabler newHostAvailabler(String projectID, List<String> hosts) throws BizException {
        return new PingHostAvailabler(projectID, hosts, new PingHostAvailabler.Config());
    }

    public HostAvailabler newHostAvailabler(List<String> hosts) throws BizException {
        return new PingHostAvailabler(hosts, new PingHostAvailabler.Config());
    }
}
