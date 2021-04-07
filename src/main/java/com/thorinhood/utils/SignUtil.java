package com.thorinhood.utils;

import com.thorinhood.data.S3Headers;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.commons.codec.digest.DigestUtils;
import software.amazon.awssdk.auth.signer.internal.SignerConstant;
import software.amazon.awssdk.auth.signer.internal.SigningAlgorithm;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.utils.BinaryUtils;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class SignUtil {

    public static final String LINE_SEPARATOR = "\n";
    public static final Set<String> IGNORE_HEADERS = Set.of("connection", "x-amzn-trace-id", "user-agent", "expect",
            "authorization");

    public static String calcPayloadSignature(FullHttpRequest request, Credential credential, String prevSignature,
                                              byte[] currentChunkData, String secretKey) {
        String stringToSign = createPayloadStringToSign(request, credential, prevSignature, currentChunkData);
        byte[] signingKey = newSingingKey(
                secretKey,
                credential.getValue(Credential.DATE),
                credential.getValue(Credential.REGION_NAME),
                credential.getValue(Credential.SERVICE_NAME));
        byte[] signature = computeSignature(stringToSign, signingKey);
        return BinaryUtils.toHex(signature);
    }

    public static String calcSignature(ParsedRequest parsedRequest, FullHttpRequest request, String secretKey) {
        String contentSha256 = "";
        if (parsedRequest.getPayloadSignType() == PayloadSignType.SINGLE_CHUNK) {
            contentSha256 = request.headers().get(S3Headers.X_AMZ_CONTENT_SHA256);
        } else if (parsedRequest.getPayloadSignType() == PayloadSignType.CHUNKED) {
            contentSha256 = PayloadSignType.CHUNKED.getValue();
        } else {
            contentSha256 = PayloadSignType.UNSIGNED_PAYLOAD.getValue();
        }

        String canonicalRequest = createCanonicalRequest(
                request,
                parsedRequest.getBucket() + parsedRequest.getKey(),
                contentSha256
        );
        String stringToSign = createStringToSign(canonicalRequest, request, parsedRequest.getCredential());

        byte[] signingKey = newSingingKey(
                secretKey,
                parsedRequest.getCredential().getValue(Credential.DATE),
                parsedRequest.getCredential().getValue(Credential.REGION_NAME),
                parsedRequest.getCredential().getValue(Credential.SERVICE_NAME));

        byte[] signature = computeSignature(stringToSign, signingKey);
        return BinaryUtils.toHex(signature);
    }

    //AbstractAws4Signer
    private static String createCanonicalRequest(FullHttpRequest request, String relativePath, String contentSha256) {

        Map<String, String> headers = new TreeMap<>();
        for (Map.Entry<String, String> entry : request.headers().entries()) {
            if (!IGNORE_HEADERS.contains(entry.getKey().toLowerCase())) {
                if (!(entry.getKey().equals("content-length") && Long.parseLong(entry.getValue()) == 0L)) {
                    headers.put(entry.getKey().toLowerCase(), entry.getValue());
                }
            }
        }

        String result = request.method().toString() +
                LINE_SEPARATOR +
                // This would optionally double url-encode the resource path
                getCanonicalizedResourcePath(relativePath, false) +
                LINE_SEPARATOR +
                // TODO
                // getCanonicalizedQueryString(request.rawQueryParameters()) +
                LINE_SEPARATOR +
                getCanonicalizedHeaderString(headers) +
                LINE_SEPARATOR +
                getSignedHeadersString(headers) +
                LINE_SEPARATOR +
                contentSha256;
        return result;
    }

    private static String createPayloadStringToSign(FullHttpRequest request, Credential credential,
                                                    String prevSignature, byte[] currentChunkData) {
        String formattedRequestSigningDateTime = request.headers().get(S3Headers.X_AMZ_DATE);
        String stringToSign = "AWS4-HMAC-SHA256-PAYLOAD" +
                LINE_SEPARATOR +
                formattedRequestSigningDateTime +
                LINE_SEPARATOR +
                credential.getCredentialWithoutAccessKey() +
                LINE_SEPARATOR +
                prevSignature +
                LINE_SEPARATOR +
                BinaryUtils.toHex(DigestUtils.sha256("")) +
                LINE_SEPARATOR +
                DigestUtils.sha256Hex(currentChunkData);
        return stringToSign;
    }

    private static String createStringToSign(String canonicalRequest, FullHttpRequest request, Credential credential) {
        String formattedRequestSigningDateTime = request.headers().get(S3Headers.X_AMZ_DATE);
        String stringToSign = "AWS4-HMAC-SHA256" +
                LINE_SEPARATOR +
                formattedRequestSigningDateTime +
                LINE_SEPARATOR +
                credential.getCredentialWithoutAccessKey() +
                LINE_SEPARATOR +
                BinaryUtils.toHex(DigestUtils.sha256(canonicalRequest));
        return stringToSign;
    }

    private static String getCanonicalizedResourcePath(String resourcePath, boolean urlEncode) {
        if (StringUtils.isEmpty(resourcePath)) {
            return "/";
        } else {
            String value = urlEncode ? SdkHttpUtils.urlEncodeIgnoreSlashes(resourcePath) : resourcePath;
            if (value.startsWith("/")) {
                return value;
            } else {
                return "/".concat(value);
            }
        }
    }

    private static byte[] computeSignature(String stringToSign, byte[] signingKey) {
        return sign(stringToSign.getBytes(StandardCharsets.UTF_8), signingKey,
                SigningAlgorithm.HmacSHA256);
    }

    private static byte[] newSingingKey(String key, String dateStamp, String regionName, String serviceName) {
        byte[] kSecret = ("AWS4" + key).getBytes(StandardCharsets.UTF_8);
        byte[] kDate = sign(dateStamp, kSecret, SigningAlgorithm.HmacSHA256);
        byte[] kRegion = sign(regionName, kDate, SigningAlgorithm.HmacSHA256);
        byte[] kService = sign(serviceName, kRegion, SigningAlgorithm.HmacSHA256);
        return sign(SignerConstant.AWS4_TERMINATOR, kService, SigningAlgorithm.HmacSHA256);
    }

    private static byte[] sign(String stringData, byte[] key, SigningAlgorithm algorithm) throws SdkClientException {
        try {
            byte[] data = stringData.getBytes(StandardCharsets.UTF_8);
            return sign(data, key, algorithm);
        } catch (Exception e) {
            throw SdkClientException.builder()
                    .message("Unable to calculate a request signature: " + e.getMessage())
                    .cause(e)
                    .build();
        }
    }

    private static byte[] sign(byte[] data, byte[] key, SigningAlgorithm algorithm) throws SdkClientException {
        try {
            Mac mac = algorithm.getMac();
            mac.init(new SecretKeySpec(key, algorithm.toString()));
            return mac.doFinal(data);
        } catch (Exception e) {
            throw SdkClientException.builder()
                    .message("Unable to calculate a request signature: " + e.getMessage())
                    .cause(e)
                    .build();
        }
    }

    private static String getCanonicalizedHeaderString(Map<String, String> headers) {
        StringBuilder result = new StringBuilder();
        headers.forEach((key, value) -> {
            result.append(key).append(":");
            if (value != null) {
                result.append(value);
            }
            result.append(LINE_SEPARATOR);
        });
        return result.toString();
    }

    private static String getSignedHeadersString(Map<String, String> headers) {
        return String.join(";", headers.keySet());
    }

}
