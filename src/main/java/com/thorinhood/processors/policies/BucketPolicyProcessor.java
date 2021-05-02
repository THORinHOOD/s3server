package com.thorinhood.processors.policies;

import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.Processor;
import com.thorinhood.utils.ParsedRequest;

public abstract class BucketPolicyProcessor extends Processor {

    public BucketPolicyProcessor(S3Driver s3Driver) {
        super(s3Driver);
    }

    @Override
    protected void checkRequest(ParsedRequest request, boolean isBucketAcl) throws S3Exception {
        S3_DRIVER.isBucketExists(request.getS3BucketPath());
        boolean aclCheckResult = S3_DRIVER.isOwner(isBucketAcl, request.getS3ObjectPathUnsafe(), request.getS3User());
        if (!aclCheckResult) {
            throw S3Exception.ACCESS_DENIED()
                    .setResource("1")
                    .setRequestId("1");
        }
        if (!request.getS3User().isRootUser()) {
            boolean policyCheckResult = S3_DRIVER.checkBucketPolicy(request.getS3BucketPath(),
                    request.getS3ObjectPathUnsafe().getKeyUnsafe(), METHOD_NAME, request.getS3User());
            if (!policyCheckResult) {
                throw S3Exception.ACCESS_DENIED()
                        .setResource("1")
                        .setRequestId("1");
            }
        }

    }

}
