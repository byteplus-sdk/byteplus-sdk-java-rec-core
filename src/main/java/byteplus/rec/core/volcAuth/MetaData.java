package byteplus.rec.core.volcAuth;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter(AccessLevel.PROTECTED)
@Getter(AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class MetaData {
    private String algorithm;
    private String credentialScope;
    private String signedHeaders;
    private String date;
    private String region;
    private String service;
}
