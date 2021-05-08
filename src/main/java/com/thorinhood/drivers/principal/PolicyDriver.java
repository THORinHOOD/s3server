package com.thorinhood.drivers.principal;

import com.thorinhood.data.S3FileBucketPath;
import com.thorinhood.data.policy.BucketPolicy;
import com.thorinhood.exceptions.S3Exception;

import java.util.Optional;


public interface PolicyDriver {

    void putBucketPolicy(S3FileBucketPath s3FileBucketPath, byte[] bytes) throws S3Exception;
    Optional<BucketPolicy> getBucketPolicy(S3FileBucketPath s3FileBucketPath) throws S3Exception;
    byte[] convertBucketPolicy(BucketPolicy bucketPolicy) throws S3Exception;

}
