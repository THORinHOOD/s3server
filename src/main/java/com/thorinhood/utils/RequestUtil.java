package com.thorinhood.utils;

import com.thorinhood.data.S3Headers;
import com.thorinhood.data.S3ResponseErrorCodes;
import com.thorinhood.exceptions.S3Exception;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

public class RequestUtil {

    private static final String META_PREFIX = "x-amz-meta-";

    public static Map<String, String> extractMetaData(FullHttpRequest request) {
        return request.headers()
                .entries()
                .stream()
                .filter(entry -> entry.getKey().startsWith(META_PREFIX))
                .collect(Collectors.toMap(
                        entry -> entry.getKey().substring(entry.getKey().indexOf(META_PREFIX) + META_PREFIX.length()),
                        Map.Entry::getValue));
    }

    public static ParsedRequest parseRequest(FullHttpRequest request) throws S3Exception {
        PayloadSignType payloadSignType = getPayloadSignType(request);
        byte[] bytes = convert(request.content().asReadOnly());
        Credential credential = Credential.parse(request);
        String requestSignature = extractSignature(request);
        String decodedContentLength = request.headers().get(S3Headers.X_AMZ_DECODED_CONTENT_LENGTH);
        String bucket = extractBucketPath(request);
        String key = extractKeyPath(request);
        return new ParsedRequest(
                bytes,
                bucket,
                key,
                requestSignature,
                credential,
                decodedContentLength != null ? Integer.parseInt(decodedContentLength) : 0,
                payloadSignType,
                request.headers());
    }

    private static String extractHeader(FullHttpRequest request, String header, String dflt) {
        if (request.headers().contains(header)) {
            return request.headers().get(header);
        }
        return dflt;
    }

    public static void checkRequest(FullHttpRequest request, ParsedRequest parsedRequest,
                                    String secretKey) throws S3Exception {
        if (parsedRequest.getPayloadSignType() == PayloadSignType.SINGLE_CHUNK) {
            String calculatedPayloadHash = DigestUtils.sha256Hex(parsedRequest.getBytes());
            if (!calculatedPayloadHash.equals(request.headers().get(S3Headers.X_AMZ_CONTENT_SHA256))) {
                throw S3Exception.build("calculated payload hash not equals with x-amz-content-sha256")
                        .setStatus(HttpResponseStatus.BAD_REQUEST)
                        .setCode(S3ResponseErrorCodes.INVALID_ARGUMENT)
                        .setMessage("x-amz-content-sha256 must be UNSIGNED-PAYLOAD, " +
                                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD, or a valid sha256 value.")
                        .setResource("1")
                        .setRequestId("1");
            }
        }

        String calculatedSignature = SignUtil.calcSignature(parsedRequest, request, secretKey);
        if (!calculatedSignature.equals(parsedRequest.getSignature())) {
            throw S3Exception.build("calculated payload hash not equals with x-amz-content-sha256")
                    .setStatus(HttpResponseStatus.BAD_REQUEST)
                    .setCode(S3ResponseErrorCodes.SIGNATURE_DOES_NOT_MATCH)
                    .setMessage("Signature is invalid")
                    .setResource("1")
                    .setRequestId("1");
        }
    }

    private static PayloadSignType getPayloadSignType(FullHttpRequest request) {
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

    private static String extractSignature(FullHttpRequest request) {
        String authorization = request.headers().get("Authorization");
        return authorization.substring(authorization.indexOf("Signature=") + "Signature=".length());
    }

    private static String extractBucketPath(FullHttpRequest request) {
        String uri = URLDecoder.decode(request.uri(), StandardCharsets.UTF_8);
        if (uri.isEmpty() || uri.charAt(0) != '/') {
            return null;
        }
        uri += "/";
        return uri.substring(1, uri.indexOf("/", 1));
    }

    private static String extractKeyPath(FullHttpRequest request) {
        String uri = URLDecoder.decode(request.uri(), StandardCharsets.UTF_8);
        if (uri.isEmpty() || uri.charAt(0) != '/') {
            return null;
        }
        int paramsIndex = uri.indexOf("?");
        return uri.replace('/', File.separatorChar).substring(uri.indexOf("/", 1),
                paramsIndex != -1 ? paramsIndex : uri.length());
    }

    private static byte[] convert(ByteBuf byteBuf) {
        byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(bytes);
        return bytes;
    }

}
