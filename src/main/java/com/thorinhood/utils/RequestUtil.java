package com.thorinhood.utils;

import com.thorinhood.data.S3Headers;
import com.thorinhood.data.S3ResponseErrorCodes;
import com.thorinhood.data.S3User;
import com.thorinhood.drivers.user.UserDriver;
import com.thorinhood.exceptions.S3Exception;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.codec.digest.DigestUtils;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

public class RequestUtil {

    private static final String META_PREFIX = "x-amz-meta-";

    private final UserDriver userDriver;

    public RequestUtil(UserDriver userDriver) {
        this.userDriver = userDriver;
    }

    public ParsedRequest parseRequest(FullHttpRequest request) throws Exception {
        PayloadSignType payloadSignType = getPayloadSignType(request);
        byte[] bytes = convert(request.content().asReadOnly());
        Credential credential = Credential.parse(request);
        String requestSignature = extractSignature(request);
        String decodedContentLength = request.headers().get(S3Headers.X_AMZ_DECODED_CONTENT_LENGTH);
        String[] bucketKey = extractBucketKey(request);
        Map<String, List<String>> queryParams = parseQueryParams(request);
        Map<String, String> metadata = extractMetaData(request);
        Optional<S3User> s3User = userDriver.getS3User(credential.getValue(Credential.ACCESS_KEY));
        if (s3User.isEmpty()) {
            throw S3Exception.ACCESS_DENIED()
                    .setResource("1")
                    .setRequestId("1");
        }
        return ParsedRequest.builder()
                .setBucket(bucketKey[0])
                .setKey(bucketKey[1])
                .setBytes(bytes)
                .setCredential(credential)
                .setDecodedContentLength(decodedContentLength != null ? Integer.parseInt(decodedContentLength) : 0)
                .setHeaders(request.headers())
                .setPayloadSignType(payloadSignType)
                .setSignature(requestSignature)
                .setQueryParams(queryParams)
                .setMethod(request.method())
                .setMetadata(metadata)
                .setS3User(s3User.get())
                .build();
    }

    public void checkRequest(ParsedRequest parsedRequest) throws S3Exception {
        if (parsedRequest.getPayloadSignType() == PayloadSignType.SINGLE_CHUNK) {
            String calculatedPayloadHash = DigestUtils.sha256Hex(parsedRequest.getBytes());
            if (!calculatedPayloadHash.equals(parsedRequest.getHeader(S3Headers.X_AMZ_CONTENT_SHA256))) {
                throw S3Exception.build("calculated payload hash not equals with x-amz-content-sha256")
                        .setStatus(HttpResponseStatus.BAD_REQUEST)
                        .setCode(S3ResponseErrorCodes.INVALID_ARGUMENT)
                        .setMessage("x-amz-content-sha256 must be UNSIGNED-PAYLOAD, " +
                                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD, or a valid sha256 value.")
                        .setResource("1")
                        .setRequestId("1");
            }
        }

        String calculatedSignature = SignUtil.calcSignature(parsedRequest);
        if (!calculatedSignature.equals(parsedRequest.getSignature())) {
            throw S3Exception.build("calculated payload hash not equals with x-amz-content-sha256")
                    .setStatus(HttpResponseStatus.BAD_REQUEST)
                    .setCode(S3ResponseErrorCodes.SIGNATURE_DOES_NOT_MATCH)
                    .setMessage("Signature is invalid")
                    .setResource("1")
                    .setRequestId("1");
        }
    }

    private Map<String, String> extractMetaData(FullHttpRequest request) {
        return request.headers()
                .entries()
                .stream()
                .filter(entry -> entry.getKey().startsWith(META_PREFIX))
                .collect(Collectors.toMap(
                        entry -> entry.getKey().substring(entry.getKey().indexOf(META_PREFIX) + META_PREFIX.length()),
                        Map.Entry::getValue));
    }

    private Map<String, List<String>> parseQueryParams(FullHttpRequest request) {
        String uri = URLDecoder.decode(request.uri(), StandardCharsets.UTF_8);
        int indexStart = uri.indexOf("?");
        if (indexStart == -1) {
            return Map.of();
        }
        String params = uri.substring(indexStart + 1);
        String[] splittedParams = params.split("&");
        Map<String, List<String>> result = new HashMap<>();
        for (String param : splittedParams) {
            int ind = param.indexOf("=");
            if (ind == -1) {
                result.put(param, new ArrayList<>());
            } else {
                String key = param.substring(0, ind);
                String values = param.substring(ind + 1);
                result.put(key, new ArrayList<>(Arrays.asList(values.split(","))));
            }
            if (result.get(param).isEmpty()) {
                result.get(param).add(null);
            }
        }
        return result;
    }

    private PayloadSignType getPayloadSignType(FullHttpRequest request) {
        if (!request.headers().contains(S3Headers.X_AMZ_CONTENT_SHA256)) {
            throw S3Exception.build("x-amz-content-sha256 header not found")
                    .setStatus(BAD_REQUEST)
                    .setCode(S3ResponseErrorCodes.INVALID_REQUEST)
                    .setMessage("Missing required header for this request: x-amz-content-sha256")
                    .setResource("1")
                    .setRequestId("1");
        }
        String value = request.headers().get(S3Headers.X_AMZ_CONTENT_SHA256);
        if (value.compareToIgnoreCase(PayloadSignType.UNSIGNED_PAYLOAD.getValue()) == 0) {
            return PayloadSignType.UNSIGNED_PAYLOAD;
        } else if (value.compareToIgnoreCase(PayloadSignType.CHUNKED.getValue()) == 0) {
            return PayloadSignType.CHUNKED;
        } else {
            return PayloadSignType.SINGLE_CHUNK;
        }
    }

    private String extractSignature(FullHttpRequest request) {
        String authorization = request.headers().get("Authorization");
        return authorization.substring(authorization.indexOf("Signature=") + "Signature=".length());
    }

    private String[] extractBucketKey(FullHttpRequest request) throws S3Exception {
        String uri = URLDecoder.decode(request.uri(), StandardCharsets.UTF_8);
        if (uri.isEmpty() || uri.charAt(0) != '/') {
            throw S3Exception.build("Incorrect uri path")
                    .setStatus(BAD_REQUEST)
                    .setCode(S3ResponseErrorCodes.INVALID_REQUEST)
                    .setMessage("Invalid uri path")
                    .setResource("1")
                    .setRequestId("1");
        }
        int paramsStart = uri.indexOf("?");
        if (paramsStart != -1) {
            uri = uri.substring(0, paramsStart);
        }
        int secondSlash = uri.indexOf("/", 1);
        if (secondSlash != -1) {
            return new String[]{ uri.substring(1, secondSlash), uri.substring(secondSlash) };
        }
        return new String[] { uri.substring(1), "" };
    }

    private byte[] convert(ByteBuf byteBuf) {
        byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(bytes);
        return bytes;
    }

}
