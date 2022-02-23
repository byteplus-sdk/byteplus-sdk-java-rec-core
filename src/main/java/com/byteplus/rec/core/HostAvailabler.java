package com.byteplus.rec.core;

public interface HostAvailabler {
    String getHost();

    String getHostByPath(String httpPath);

    void shutdown();
}
