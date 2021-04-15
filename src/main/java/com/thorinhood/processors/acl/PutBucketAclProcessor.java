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

    public PutBucketAclProcessor(String basePath, S3Driver s3Driver) {
        super(basePath, s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object... arguments) throws Exception {
        if (!S3_DRIVER.checkBucketPermission(BASE_PATH, parsedRequest.getBucket(), METHOD_NAME,
                parsedRequest.getS3User())) {
            throw S3Exception.ACCESS_DENIED()
                    .setResource("1")
                    .setRequestId("1"); // TODO
        }
        S3_DRIVER.putBucketAcl(BASE_PATH, parsedRequest.getBucket(), parsedRequest.getBytes());
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
