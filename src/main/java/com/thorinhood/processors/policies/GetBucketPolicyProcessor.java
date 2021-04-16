package com.thorinhood.processors.policies;

import com.thorinhood.data.S3ResponseErrorCodes;
import com.thorinhood.data.policy.BucketPolicy;
import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.Processor;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.ParsedRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class GetBucketPolicyProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(PutBucketPolicyProcessor.class);

    public GetBucketPolicyProcessor(String basePath, S3Driver s3Driver) {
        super(basePath, s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object... arguments) throws Exception {
        checkRequestPermission(parsedRequest, true);
        Optional<byte[]> bucketPolicy = S3_DRIVER.getBucketPolicyBytes(parsedRequest.getBucket());
        if (bucketPolicy.isEmpty()) {
            throw S3Exception.build("The bucket policy does not exist")
                    .setStatus(HttpResponseStatus.NOT_FOUND)
                    .setCode(S3ResponseErrorCodes.INVALID_REQUEST)
                    .setMessage("The bucket policy does not exist")
                    .setResource("1")
                    .setRequestId("1"); // TODO
        }
        sendResponse(context, request, OK, response -> {
            HttpUtil.setContentLength(response, bucketPolicy.get().length);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
            response.headers().set("Date", DateTimeUtil.currentDateTime());
        }, bucketPolicy.get());
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
