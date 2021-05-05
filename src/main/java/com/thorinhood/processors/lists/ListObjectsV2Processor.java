package com.thorinhood.processors.lists;

import com.thorinhood.data.GetBucketObjects;
import com.thorinhood.data.results.ListBucketResult;
import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.processors.Processor;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.ParsedRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class ListObjectsV2Processor extends Processor {

    private static final Logger log = LogManager.getLogger(ListObjectsV2Processor.class);

    public ListObjectsV2Processor(S3Driver s3Driver) {
        super(s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object... arguments) throws Exception {
        checkRequest(parsedRequest, "s3:ListBucket", true);
        GetBucketObjects getBucketObjects = GetBucketObjects.builder()
                .setBucket(parsedRequest.getS3BucketPath().getBucket())
                .setPrefix(parsedRequest.getQueryParam("prefix", null, Function.identity()))
                .setMaxKeys(parsedRequest.getQueryParam("max-keys", 1000, Integer::valueOf))
                .setStartAfter(parsedRequest.getQueryParam("start-after", null, Function.identity()))
                .setContinuationToken(parsedRequest.getQueryParam("continuation-token", null,
                        Function.identity()))
                .build();
        ListBucketResult listBucketResult = S3_DRIVER.getBucketObjects(getBucketObjects);
        String xml = listBucketResult.buildXmlText();
        sendResponse(context, request, OK, response -> {
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
