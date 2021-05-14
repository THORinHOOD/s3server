package com.thorinhood.processors.actions;

import com.thorinhood.data.requests.S3ResponseErrorCodes;
import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.Processor;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.ParsedRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Map;

public class CreateBucketProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(CreateBucketProcessor.class);

    public CreateBucketProcessor(S3Driver s3Driver) {
        super(s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object[] arguments) throws Exception {
        if (!isBucketNameCorrect(parsedRequest.getS3BucketPath().getBucket())) {
            throw S3Exception.build("Illegal bucket name : " + parsedRequest.getS3BucketPath()
                        .getBucket())
                    .setStatus(HttpResponseStatus.BAD_REQUEST)
                    .setCode(S3ResponseErrorCodes.INVALID_ARGUMENT)
                    .setMessage("Illegal bucket name : " + parsedRequest.getS3BucketPath().getBucket())
                    .setResource("1")
                    .setRequestId("1");
        }
        S3_DRIVER.createBucket(parsedRequest.getS3BucketPath(), parsedRequest.getS3User());
        sendResponseWithoutContent(context, HttpResponseStatus.OK, request, Map.of(
                "Date", DateTimeUtil.currentDateTime(),
                "Location", File.separatorChar + parsedRequest.getS3BucketPath().getBucket()
        ));
    }

    private boolean isBucketNameCorrect(String bucket) {
        if (bucket == null) {
            return false;
        }
        if (bucket.length() < 3 || bucket.length() > 63) {
            return false;
        }
        for (int i = 0; i < bucket.length(); i++) {
            char c = bucket.charAt(i);
            if (!((Character.isLetter(c) && Character.isLowerCase(c)) || Character.isDigit(c) || c == '.' || c == '-')) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
