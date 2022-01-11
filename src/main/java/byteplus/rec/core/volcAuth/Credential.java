package byteplus.rec.core.volcAuth;

import lombok.*;

@Setter(AccessLevel.PROTECTED)
@Getter(AccessLevel.PROTECTED)
public class Credential {
    private String accessKeyID;
    private String secretAccessKey;
    private String service;
    private String region;
    private String sessionToken;

    public Credential(String ak, String sk, String service, String region) {
        this.accessKeyID = ak;
        this.secretAccessKey = sk;
        this.region = region;
        this.service = service;
    }
}
