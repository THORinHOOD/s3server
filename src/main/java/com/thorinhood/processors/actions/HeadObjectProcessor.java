package com.thorinhood.processors.actions;

import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.exceptions.S3Exception;
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

import java.io.IOException;
import java.util.Map;

public class HeadObjectProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(HeadObjectProcessor.class);

    public HeadObjectProcessor(S3Driver s3Driver) {
        super(s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object... arguments) throws Exception {
        checkRequestPermissions(parsedRequest, "s3:GetObject", false);
        S3Object s3Object = S3_DRIVER.headObject(parsedRequest.getS3ObjectPath(), parsedRequest.getHeaders());
        sendResponse(context, request, HttpResponseStatus.OK, response -> {
            HttpUtil.setContentLength(response, s3Object.getFile().length());
            try {
                setContentTypeHeader(response, s3Object.getFile());
            } catch (IOException exception) {
                sendError(context, request, S3Exception.INTERNAL_ERROR(exception)
                    .setResource("1")
                    .setRequestId("1"));
            }
            response.headers().set("ETag", s3Object.getETag());
            response.headers().set("Last-Modified", s3Object.getLastModified());
            response.headers().set("Date", DateTimeUtil.currentDateTime());
            s3Object.getMetaData().forEach((metaKey, metaValue) ->
                    response.headers().set("x-amz-meta-" + metaKey, metaValue));
        });
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
