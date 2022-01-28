package byteplus.rec.core;

import lombok.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RegionHelper {
    protected static final String REGION_UNKNOWN = "";
    private static final Map<String, RegionConfig> regionConfigMap = new HashMap<>();

    @Setter
    @Getter(AccessLevel.PROTECTED)
    @AllArgsConstructor
    @NoArgsConstructor
    public static class RegionConfig {
        private List<String> hosts;
        private String volcCredentialRegion;
    }

    public static void registerRegion(String region, RegionConfig regionConfig) {
        RegionConfig config = regionConfigMap.get(region);
        if (config != null) {
            throw new RuntimeException(String.format("region has already exist: %s", region));
        }
        regionConfigMap.put(region, regionConfig);
    }

    protected static RegionConfig getRegionConfig(String region) {
        return regionConfigMap.get(region);
    }

    protected static List<String> getRegionHosts(String region) {
        return regionConfigMap.get(region).getHosts();
    }

    protected static String getVolcCredentialRegion(String region) {
        return regionConfigMap.get(region).getVolcCredentialRegion();
    }
}
