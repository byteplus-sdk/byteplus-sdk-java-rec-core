package com.byteplus.rec.core;

public interface HostAvailabler {
    String getHost(String httpPath);

    void shutdown();
}
