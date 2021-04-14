package com.thorinhood.processors;

import com.thorinhood.utils.ParsedRequest;
import com.thorinhood.drivers.S3Driver;
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

    public CreateBucketProcessor(String basePath, S3Driver s3Driver) {
        super(basePath, s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object[] arguments)
            throws Exception {
        try {
            S3_DRIVER.createBucket(parsedRequest.getBucket(), BASE_PATH);
            sendResponseWithoutContent(context, HttpResponseStatus.OK, request, Map.of(
                "Date", DateTimeUtil.currentDateTime(),
                "Location", File.separatorChar + parsedRequest.getBucket()
            ));
        } catch (S3Exception exception) {
            sendError(context, request, exception);
            log.error(exception.getMessage(), exception);
        }
    }
}
