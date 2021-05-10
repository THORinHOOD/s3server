package com.thorinhood.processors.actions;

import com.thorinhood.data.S3FileObjectPath;
import com.thorinhood.data.results.CopyObjectResult;
import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.processors.Processor;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.ParsedRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CopyObjectProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(CopyObjectProcessor.class);

    public CopyObjectProcessor(S3Driver s3Driver) {
        super(s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object... arguments) throws Exception {
        S3FileObjectPath sourcePath = S3_DRIVER.buildPathToObject(parsedRequest.getHeader("x-amz-copy-source"));
        checkRequestPermissions(sourcePath, parsedRequest.getS3User(), "s3:GetObject", false);
        checkRequestPermissions(parsedRequest, "s3:PutObject", true);
        CopyObjectResult copyObjectResult = S3_DRIVER.copyObject(
                sourcePath,
                parsedRequest.getS3ObjectPath(),
                parsedRequest.getS3User());
        String xml = copyObjectResult.buildXmlText();
        sendResponse(context, request, HttpResponseStatus.OK, response -> {
            response.headers().set("Date", DateTimeUtil.currentDateTime());
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, xml.getBytes().length);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/xml");
        }, xml);
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

}
