package com.byteplus.rec.core;

import java.util.List;

public interface HostAvailabler {
    String getHost(String httpPath);

    List<String> getHosts();

    void shutdown();
}
