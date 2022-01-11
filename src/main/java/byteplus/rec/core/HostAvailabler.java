package byteplus.rec.core;

import java.util.List;

public interface HostAvailabler {
    List<String> getAvailableHosts();
    List<String> Hosts();
    void setHosts(List<String> hosts);
    String GetHost();
    void Shutdown();
}
