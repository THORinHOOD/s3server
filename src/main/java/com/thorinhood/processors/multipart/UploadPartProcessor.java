package com.thorinhood.processors.multipart;

import com.thorinhood.data.requests.S3ResponseErrorCodes;
import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.Processor;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.ParsedRequest;
import com.thorinhood.utils.PayloadSignType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.function.Function;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class UploadPartProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(UploadPartProcessor.class);

    public UploadPartProcessor(S3Driver s3Driver) {
        super(s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object... arguments) throws Exception {
        checkRequestPermissions(parsedRequest, "s3:PutObject", true);
        if (parsedRequest.getPayloadSignType() != PayloadSignType.SINGLE_CHUNK &&
                parsedRequest.getPayloadSignType() != PayloadSignType.UNSIGNED_PAYLOAD) {
            parsedRequest.setBytes(processChunkedContent(parsedRequest, request));
        }
        int partNumber = parsedRequest.getQueryParam("partNumber", null, Integer::valueOf);
        if (partNumber < 1 || partNumber > 10000) {
            throw S3Exception.build("Part number must be an integer between 1 and 10000, inclusive")
                    .setStatus(HttpResponseStatus.BAD_REQUEST)
                    .setCode(S3ResponseErrorCodes.INVALID_ARGUMENT)
                    .setMessage("Part number must be an integer between 1 and 10000, inclusive")
                    .setResource("1")
                    .setRequestId("1");
        }
        String eTag = S3_DRIVER.putUploadPart(
                parsedRequest.getS3ObjectPath(),
                parsedRequest.getQueryParam("uploadId", null, Function.identity()),
                partNumber,
                parsedRequest.getBytes());
        sendResponseWithoutContent(context, OK, request, Map.of(
        "ETag", eTag,
            HttpHeaderNames.CONTENT_LENGTH.toString(), 0,
        "Date", DateTimeUtil.currentDateTime()
        ));
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
