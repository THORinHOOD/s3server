package com.thorinhood.processors.lists;

import com.thorinhood.data.list.eventual.ListBucketResult;
import com.thorinhood.data.list.request.GetBucketObjects;
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

public class ListObjectsProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(ListObjectsProcessor.class);

    public ListObjectsProcessor(S3Driver s3Driver) {
        super(s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object... arguments) throws Exception {
        checkRequestPermissions(parsedRequest, "s3:ListBucket", true);
        GetBucketObjects getBucketObjects = GetBucketObjects.builder()
                .setBucket(parsedRequest.getS3BucketPath().getBucket())
                .setPrefix(parsedRequest.getQueryParam("prefix", "", Function.identity()))
                .setDelimiter(parsedRequest.getQueryParam("delimiter", null, delimiter ->
                        delimiter.equals("") ? null : delimiter))
                .setMarker(parsedRequest.getQueryParam("marker", null, Function.identity()))
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
