package byteplus.rec.core;

import java.util.List;

public interface HostAvailabler {
    List<String> getAvailableHosts();
    List<String> hosts();
    void setHosts(List<String> hosts);
    String hostHeader();
    void setHostHeader(String hostHeader);
    String getHost();
    void shutdown();
}
