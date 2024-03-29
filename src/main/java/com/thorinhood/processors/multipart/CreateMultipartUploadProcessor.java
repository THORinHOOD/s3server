package com.thorinhood.processors.multipart;

import com.thorinhood.data.results.InitiateMultipartUploadResult;
import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.processors.Processor;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.ParsedRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;

public class CreateMultipartUploadProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(CreateMultipartUploadProcessor.class);

    public CreateMultipartUploadProcessor(S3Driver s3Driver) {
        super(s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object... arguments) throws Exception {
        checkRequestPermissions(parsedRequest, "s3:PutObject", true);
        String uploadId = S3_DRIVER.createMultipartUpload(parsedRequest.getS3ObjectPath(), parsedRequest.getS3User());
        InitiateMultipartUploadResult result = InitiateMultipartUploadResult.builder()
                .setBucket(parsedRequest.getS3ObjectPath().getBucket())
                .setKey(parsedRequest.getS3ObjectPath().getKey())
                .setUploadId(uploadId)
                .build();
        String xml = result.buildXmlText();
        sendResponse(context, request, HttpResponseStatus.OK, response -> {
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/xml");
            response.headers().set("Date", DateTimeUtil.currentDateTime());
        }, xml);
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
