package com.thorinhood.processors;

import com.thorinhood.utils.S3Util;
import com.thorinhood.exceptions.S3Exception;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.thorinhood.utils.DateTimeUtil;

import java.io.File;
import java.util.Map;

public class CreateBucketProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(CreateBucketProcessor.class);

    public CreateBucketProcessor(String basePath, S3Util s3Util) {
        super(basePath, s3Util);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, Object[] arguments)
            throws Exception {
        String bucket = extractBucket(request);
        try {
            S3_UTIL.createBucket(bucket, BASE_PATH);
            sendResponseWithoutContent(context, HttpResponseStatus.OK, request, Map.of(
                "Date", DateTimeUtil.currentDateTime(),
                "Location", File.separatorChar + bucket
            ));
        } catch (S3Exception exception) {
            sendError(context, request, exception);
            log.error(exception.getMessage(), exception);
        }
    }
}
