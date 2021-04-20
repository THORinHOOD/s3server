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
import java.util.*;

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

    public static String calcSignature(ParsedRequest parsedRequest) {
        String contentSha256 = "";
        if (parsedRequest.getPayloadSignType() == PayloadSignType.SINGLE_CHUNK) {
            contentSha256 = parsedRequest.getHeader(S3Headers.X_AMZ_CONTENT_SHA256);
        } else if (parsedRequest.getPayloadSignType() == PayloadSignType.CHUNKED) {
            contentSha256 = PayloadSignType.CHUNKED.getValue();
        } else {
            contentSha256 = PayloadSignType.UNSIGNED_PAYLOAD.getValue();
        }

        String canonicalRequest = createCanonicalRequest(
                parsedRequest,
                parsedRequest.getRawUri(),
                contentSha256
        );
        String stringToSign = createStringToSign(canonicalRequest, parsedRequest);

        byte[] signingKey = newSingingKey(
                parsedRequest.getS3User().getSecretKey(),
                parsedRequest.getCredential().getValue(Credential.DATE),
                parsedRequest.getCredential().getValue(Credential.REGION_NAME),
                parsedRequest.getCredential().getValue(Credential.SERVICE_NAME));

        byte[] signature = computeSignature(stringToSign, signingKey);
        return BinaryUtils.toHex(signature);
    }

    private static String createCanonicalRequest(ParsedRequest parsedRequest, String relativePath,
                                                 String contentSha256) {
        Map<String, String> headers = new TreeMap<>();
        for (Map.Entry<String, String> entry : parsedRequest.getHeaders().entries()) {
            if (!IGNORE_HEADERS.contains(entry.getKey().toLowerCase())) {
                if (!(entry.getKey().equals("content-length") && Long.parseLong(entry.getValue()) == 0L)) {
                    headers.put(entry.getKey().toLowerCase(), entry.getValue());
                }
            }
        }
        return parsedRequest.getMethod().toString() +
                LINE_SEPARATOR +
                getCanonicalizedResourcePath(relativePath, false) +
                LINE_SEPARATOR +
                getCanonicalizedQueryString(parsedRequest.getQueryParams()) +
                LINE_SEPARATOR +
                getCanonicalizedHeaderString(headers) +
                LINE_SEPARATOR +
                getSignedHeadersString(headers) +
                LINE_SEPARATOR +
                contentSha256;
    }

    private static String createPayloadStringToSign(FullHttpRequest request, Credential credential,
                                                    String prevSignature, byte[] currentChunkData) {
        String formattedRequestSigningDateTime = request.headers().get(S3Headers.X_AMZ_DATE);
        return "AWS4-HMAC-SHA256-PAYLOAD" +
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
    }

    private static String createStringToSign(String canonicalRequest, ParsedRequest parsedRequest) {
        return "AWS4-HMAC-SHA256" +
                LINE_SEPARATOR +
                parsedRequest.getHeader(S3Headers.X_AMZ_DATE) +
                LINE_SEPARATOR +
                parsedRequest.getCredential().getCredentialWithoutAccessKey() +
                LINE_SEPARATOR +
                BinaryUtils.toHex(DigestUtils.sha256(canonicalRequest));
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

    private static String getCanonicalizedQueryString(Map<String, List<String>> parameters) {
        SortedMap<String, List<String>> sorted = new TreeMap<>();
        for (Map.Entry<String, List<String>> entry : parameters.entrySet()) {
            String encodedParamName = SdkHttpUtils.urlEncode(entry.getKey());
            List<String> paramValues = entry.getValue();
            List<String> encodedValues = new ArrayList<>(paramValues.size());
            for (String value : paramValues) {
                String encodedValue = SdkHttpUtils.urlEncode(value);
                String signatureFormattedEncodedValue = encodedValue == null ? "" : encodedValue;
                encodedValues.add(signatureFormattedEncodedValue);
            }
            Collections.sort(encodedValues);
            sorted.put(encodedParamName, encodedValues);

        }
        return SdkHttpUtils.flattenQueryParameters(sorted).orElse("");
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
