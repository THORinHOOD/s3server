package com.thorinhood.processors.acl;

import com.thorinhood.acl.AccessControlPolicy;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.Processor;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.ParsedRequest;
import com.thorinhood.drivers.S3Driver;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class GetBucketAclProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(GetObjectAclProcessor.class);

    public GetBucketAclProcessor(String basePath, S3Driver s3Driver) {
        super(basePath, s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object... arguments) throws Exception {
        try {
            AccessControlPolicy accessControlPolicy = S3_DRIVER.getBucketAcl(BASE_PATH, parsedRequest.getBucket());
            String xml = accessControlPolicy.buildXmlText();
            sendResponse(context, request, OK, response -> {
                HttpUtil.setContentLength(response, xml.getBytes(StandardCharsets.UTF_8).length);
                response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
                //        response.headers().set("Last-Modified", s3Object.getLastModified()); // TODO
                response.headers().set("Date", DateTimeUtil.currentDateTime());
            }, xml);
        } catch (S3Exception s3Exception) {
            sendError(context, request, s3Exception);
            log.error(s3Exception.getMessage(), s3Exception);
            return;
        }
    }

}
