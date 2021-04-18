package com.thorinhood.processors.acl;

import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.Processor;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.ParsedRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class PutBucketAclProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(PutBucketAclProcessor.class);

    public PutBucketAclProcessor(S3Driver s3Driver) {
        super(s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object... arguments) throws Exception {
        checkRequestPermission(parsedRequest, true);
        S3_DRIVER.putBucketAcl(parsedRequest.getBucket(), parsedRequest.getBytes());
        sendResponseWithoutContent(context, OK, request, Map.of(
                "Date", DateTimeUtil.currentDateTime(),
                "Content-Length", 0
        ));
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
