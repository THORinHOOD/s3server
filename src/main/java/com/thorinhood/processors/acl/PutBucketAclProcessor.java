package com.thorinhood.processors.acl;

import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.Processor;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.ParsedRequest;
import com.thorinhood.drivers.S3Driver;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class PutBucketAclProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(PutBucketAclProcessor.class);

    public PutBucketAclProcessor(String basePath, S3Driver s3Driver) {
        super(basePath, s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object... arguments) throws Exception {
        try {
            S3_DRIVER.putBucketAcl(BASE_PATH, parsedRequest.getBucket(), parsedRequest.getBytes());
            sendResponseWithoutContent(context, OK, request, Map.of(
                    "Date", DateTimeUtil.currentDateTime(),
                    "Content-Length", 0
            ));
        } catch (S3Exception s3Exception) {
            sendError(context, request, s3Exception);
            log.error(s3Exception.getMessage(), s3Exception);
        }
    }
}
