package byteplus.rec.core;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.Request;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class VoclAuth {
    private static final TimeZone tz = TimeZone.getTimeZone("UTC");
    private static final Set<String> H_INCLUDE = new HashSet<>();
    private static final String TIME_FORMAT_V4 = "yyyyMMdd'T'HHmmss'Z'";

    static {
        H_INCLUDE.add("Content-Type");
        H_INCLUDE.add("Content-Md5");
        H_INCLUDE.add("Host");
    }

    protected static Headers sign(Request request, byte[] reqBytes, Credential credential) throws Exception {
        Headers.Builder headerBuilder = request.headers().newBuilder();

        String formatDate = getCurrentFormatDate();
        headerBuilder.set("X-Date", formatDate);

        MetaData meta = new MetaData();
        meta.setAlgorithm("HMAC-SHA256");
        meta.setService(credential.getService());
        meta.setRegion(credential.getRegion());
        meta.setDate(toDate(formatDate));

        // step 1 hash request body
        String bodyHash = hashSHA256(reqBytes);
        headerBuilder.set("X-Content-Sha256", bodyHash);
        // step 2 generate signature
        meta.setCredentialScope(String.join(
                "/", new String[]{
                        meta.getDate(),
                        meta.getRegion(),
                        meta.getService(),
                        "request"
                }));

        String stringToSign = String.join(
                "\n", new String[]{
                        meta.getAlgorithm(),
                        formatDate,
                        meta.getCredentialScope(),
                        hashCanonicalRequest(request, bodyHash, meta),
                });

        // step 3 hash signature
        byte[] signingKey = genSigningSecretKeyV4(
                credential.getSecretAccessKey(), meta.getDate(), meta.getRegion(), meta.getService());

        String signature = Helper.bytes2Hex(hmacSHA256(signingKey, stringToSign));
        headerBuilder.set("Authorization", buildAuthHeaderV4(signature, meta, credential));
        return headerBuilder.build();
    }

    private static String hashCanonicalRequest(Request request, String bodyHash, MetaData meta) throws Exception {
        List<String> signedHeaders = new ArrayList<>();
        for (String headerName : request.headers().names()) {
            if (H_INCLUDE.contains(headerName) || headerName.startsWith("X-")) {
                signedHeaders.add(headerName.toLowerCase());
            }
        }
        Collections.sort(signedHeaders);
        StringBuilder signedHeadersToSignStr = new StringBuilder();
        for (String header : signedHeaders) {
            String value = Objects.requireNonNull(request.header(header)).trim();
            if (header.equals("host")) {
                if (value.contains(":")) {
                    String[] split = value.split(":");
                    String port = split[1];
                    if (port.equals("80") || port.equals("443")) {
                        value = split[0];
                    }
                }
            }
            signedHeadersToSignStr.append(header).append(":").append(value).append("\n");
        }

        meta.setSignedHeaders(String.join(";", signedHeaders));

        String canonicalRequest = String.join(
                "\n", new String[]{
                        request.method(),
                        normUri(request.url().encodedPath()),
                        normQuery(request.url()),
                        signedHeadersToSignStr.toString(),
                        meta.getSignedHeaders(),
                        bodyHash
                });

        return hashSHA256(canonicalRequest.getBytes());
    }

    private static byte[] genSigningSecretKeyV4(String secretKey, String date, String region, String service) throws Exception {
        byte[] kDate = hmacSHA256((secretKey).getBytes(), date);
        byte[] kRegion = hmacSHA256(kDate, region);
        byte[] kService = hmacSHA256(kRegion, service);
        return hmacSHA256(kService, "request");
    }

    private static String buildAuthHeaderV4(String signature, MetaData meta, Credential credentials) {
        String credential = credentials.getAccessKeyID() + "/" + meta.getCredentialScope();

        return meta.getAlgorithm() + " Credential=" + credential +
                ", SignedHeaders=" + meta.getSignedHeaders() + ", Signature=" + signature;
    }

    private static String getCurrentFormatDate() {
        DateFormat df = new SimpleDateFormat(TIME_FORMAT_V4);
        df.setTimeZone(tz);
        return df.format(new Date());
    }

    private static String toDate(String timestamp) {
        return timestamp.substring(0, 8);
    }

    private static String normUri(String encodedPath) {
        return encodedPath.replace("%2F", "/").replace("+", "%20");
    }

    private static String normQuery(HttpUrl url) {
        final HttpUrl.Builder urlBuilder = url.newBuilder();
        final HttpUrl finalUrl = url;
        url.queryParameterNames()
                .stream()
                .sorted()
                .forEach(queryName -> {
                    urlBuilder.setQueryParameter(queryName, finalUrl.queryParameter(queryName));
                });
        String sortedQuery = urlBuilder.build().encodedQuery();
        if (Objects.nonNull(sortedQuery)) {
            return sortedQuery.replace("+", "%20");
        }
        return "";
    }

    private static String hashSHA256(byte[] content) throws Exception {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return Helper.bytes2Hex(md.digest(content));
        } catch (Exception e) {
            throw new Exception(
                    "Unable to compute hash while signing request: "
                            + e.getMessage(), e);
        }
    }

    private static byte[] hmacSHA256(byte[] key, String content) throws Exception {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(content.getBytes());
        } catch (Exception e) {
            throw new Exception(
                    "Unable to calculate a request signature: "
                            + e.getMessage(), e);
        }
    }

    @Setter(AccessLevel.PROTECTED)
    @Getter(AccessLevel.PROTECTED)
    protected static class Credential {
        private String accessKeyID;
        private String secretAccessKey;
        private String service;
        private String region;
        private String sessionToken;

        protected Credential(String ak, String sk, String service, String region) {
            this.accessKeyID = ak;
            this.secretAccessKey = sk;
            this.region = region;
            this.service = service;
        }
    }


    @Setter(AccessLevel.PRIVATE)
    @Getter(AccessLevel.PRIVATE)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    private static class MetaData {
        private String algorithm;
        private String credentialScope;
        private String signedHeaders;
        private String date;
        private String region;
        private String service;
    }
}
