package com.thorinhood.processors.lists;

import com.thorinhood.data.GetBucketObjectsRequest;
import com.thorinhood.data.ListBucketResult;
import com.thorinhood.data.S3Content;
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
import java.util.List;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class ListObjectsProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(ListObjectsProcessor.class);

    public ListObjectsProcessor(S3Driver s3Driver) {
        super(s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object... arguments) throws Exception {
        checkRequest(parsedRequest, "s3:ListBucket", true);
        GetBucketObjectsRequest getBucketObjectsRequest = GetBucketObjectsRequest.builder()
                .setBucket(parsedRequest.getBucket())
                .setMaxKeys(parsedRequest.getQueryParam("max-keys", 1000, Integer::valueOf))
                .build();
        List<S3Content> s3Contents = S3_DRIVER.getBucketObjects(getBucketObjectsRequest);
        ListBucketResult listBucketResult = ListBucketResult.builder()
                .setMaxKeys(getBucketObjectsRequest.getMaxKeys())
                .setName(parsedRequest.getBucket())
                .setContents(s3Contents)
                .build();
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
