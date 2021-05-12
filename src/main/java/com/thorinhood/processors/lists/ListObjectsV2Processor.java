package com.thorinhood.processors.lists;

import com.thorinhood.data.list.request.GetBucketObjectsV2;
import com.thorinhood.data.list.eventual.ListBucketV2Result;
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
        checkRequestPermissions(parsedRequest, "s3:ListBucket", true);
        GetBucketObjectsV2 getBucketObjectsV2 = GetBucketObjectsV2.builder()
                .setBucket(parsedRequest.getS3BucketPath().getBucket())
                .setPrefix(parsedRequest.getQueryParam("prefix", "", Function.identity()))
                .setMaxKeys(parsedRequest.getQueryParam("max-keys", 1000, Integer::valueOf))
                .setStartAfter(parsedRequest.getQueryParam("start-after", null, Function.identity()))
                .setContinuationToken(parsedRequest.getQueryParam("continuation-token", null,
                        Function.identity()))
                .setDelimiter(parsedRequest.getQueryParam("delimiter", null, delimiter ->
                        delimiter.equals("") ? null : delimiter))
                .build();
        ListBucketV2Result listBucketV2Result = S3_DRIVER.getBucketObjectsV2(getBucketObjectsV2);
        String xml = listBucketV2Result.buildXmlText();
        sendResponse(context, request, OK, response -> {
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/xml");
            response.headers().set("Date", DateTimeUtil.currentDateTime());
        }, xml);
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
