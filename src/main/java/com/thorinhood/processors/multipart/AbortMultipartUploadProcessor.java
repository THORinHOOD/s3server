package com.thorinhood.processors.multipart;

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

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class AbortMultipartUploadProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(AbortMultipartUploadProcessor.class);

    public AbortMultipartUploadProcessor(S3Driver s3Driver) {
        super(s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object... arguments) throws Exception {
        checkRequestPermissions(parsedRequest, true);
        S3_DRIVER.abortMultipartUpload(parsedRequest.getS3ObjectPath(),
                parsedRequest.getQueryParam("uploadId", null, Function.identity()));
        sendResponseWithoutContent(context, HttpResponseStatus.NO_CONTENT, request, Map.of(
                "Date", DateTimeUtil.currentDateTime()
        ));
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    protected void checkRequestPermissions(ParsedRequest request, boolean isBucketAcl) throws S3Exception {
        S3_DRIVER.isBucketExists(request.getS3BucketPath());

        Optional<Boolean> policyCheckResult = S3_DRIVER.checkBucketPolicy(request.getS3BucketPath(),
                request.getS3ObjectPathUnsafe().getKeyUnsafe(), METHOD_NAME, request.getS3User());
        if (policyCheckResult.isPresent() && !policyCheckResult.get()) {
            throw S3Exception.ACCESS_DENIED();
        } else if (policyCheckResult.isPresent()) {
            return;
        }

        String uploadId = request.getQueryParam("uploadId", "", Function.identity());
        String accessKeyInitiator = uploadId.substring(uploadId.indexOf("_") + 1);
        if (request.getS3User().isRootUser()) {
            boolean aclCheckResult = S3_DRIVER.isOwner(isBucketAcl, request.getS3ObjectPathUnsafe(),
                    request.getS3User());
            if (!aclCheckResult) {
                throw S3Exception.ACCESS_DENIED();
            }
        } else if (!(request.getS3User().getAccessKey().equals(accessKeyInitiator))) {
            throw S3Exception.ACCESS_DENIED();
        }
    }
}
