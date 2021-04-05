package com.thorinhood.utils;

import com.thorinhood.data.S3Headers;
import com.thorinhood.data.S3ResponseErrorCodes;
import com.thorinhood.data.PayloadSignType;
import com.thorinhood.exceptions.S3Exception;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.Map;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

public class RequestWorker {

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

    public static PayloadSignType getPayloadSignType(FullHttpRequest request) {
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

    public static boolean checkPayload(PayloadSignType payloadSignType, byte[] payload, String xAmzContentSha256)
            throws S3Exception {
        if (payloadSignType == PayloadSignType.UNSIGNED_PAYLOAD) {
            return true;
        } else if (payloadSignType == PayloadSignType.SINGLE_CHUNK) {
            String calculatedPayloadHash = DigestUtils.sha256Hex(payload);
            if (!calculatedPayloadHash.equals(xAmzContentSha256)) {
                throw S3Exception.build("calculated payload hash not equals with x-amz-content-sha256")
                        .setStatus(HttpResponseStatus.BAD_REQUEST)
                        .setCode(S3ResponseErrorCodes.INVALID_ARGUMENT)
                        .setMessage("x-amz-content-sha256 must be UNSIGNED-PAYLOAD, " +
                                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD, or a valid sha256 value.")
                        .setResource("1")
                        .setRequestId("1");
            }
            return true;
        }
        return false;
    }

}
