package com.thorinhood.processors.lists;

import com.thorinhood.data.GetBucketsResult;
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

public class ListBucketsProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(ListBucketsProcessor.class);

    public ListBucketsProcessor(S3Driver s3Driver) {
        super(s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object... arguments) throws Exception {
        GetBucketsResult result = S3_DRIVER.getBuckets(parsedRequest.getS3User());
        String xml = result.buildXmlText();
        sendResponse(context, request, HttpResponseStatus.OK, response -> {
            HttpUtil.setContentLength(response, xml.getBytes(StandardCharsets.UTF_8).length);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/xml");
            response.headers().set("Date", DateTimeUtil.currentDateTime());
        }, xml);
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

}
