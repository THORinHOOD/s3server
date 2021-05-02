package com.thorinhood.drivers.principal;

import com.thorinhood.data.S3BucketPath;
import com.thorinhood.data.policy.BucketPolicy;
import com.thorinhood.exceptions.S3Exception;

import java.util.Optional;


public interface PolicyDriver {

    void putBucketPolicy(S3BucketPath s3BucketPath, BucketPolicy bucketPolicy) throws S3Exception;
    void putBucketPolicy(S3BucketPath s3BucketPath, byte[] bytes) throws S3Exception;
    Optional<BucketPolicy> getBucketPolicy(S3BucketPath s3BucketPath) throws S3Exception;
    byte[] convertBucketPolicy(BucketPolicy bucketPolicy) throws S3Exception;

}
