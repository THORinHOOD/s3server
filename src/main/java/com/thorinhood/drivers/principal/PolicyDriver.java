package com.thorinhood.drivers.principal;

import com.thorinhood.data.policy.BucketPolicy;
import com.thorinhood.exceptions.S3Exception;

import java.util.Optional;


public interface PolicyDriver {

    void putBucketPolicy(String bucket, BucketPolicy bucketPolicy) throws S3Exception;
    void putBucketPolicy(String bucket, byte[] bytes) throws S3Exception;
    Optional<BucketPolicy> getBucketPolicy(String bucket) throws S3Exception;
    byte[] convertBucketPolicy(BucketPolicy bucketPolicy) throws S3Exception;

}
