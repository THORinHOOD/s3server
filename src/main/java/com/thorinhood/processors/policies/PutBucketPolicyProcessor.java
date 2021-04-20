package com.thorinhood.processors.policies;

import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.ParsedRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class PutBucketPolicyProcessor extends BucketPolicyProcessor {

    private static final Logger log = LogManager.getLogger(PutBucketPolicyProcessor.class);

    public PutBucketPolicyProcessor(S3Driver s3Driver) {
        super(s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object... arguments) throws Exception {
        checkRequest(parsedRequest, true);
        S3_DRIVER.putBucketPolicy(parsedRequest.getBucket(), parsedRequest.getBytes());
        sendResponseWithoutContent(context, OK, request, Map.of(
                "Date", DateTimeUtil.currentDateTime()
        ));
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

}
