package com.thorinhood.drivers.main;

import com.thorinhood.data.S3User;
import com.thorinhood.data.acl.AccessControlPolicy;
import com.thorinhood.data.policy.BucketPolicy;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.utils.ParsedRequest;

import java.util.Map;
import java.util.Optional;

public interface S3Driver extends AclPermissionChecker {

    // Permissions
    AccessControlPolicy getBucketAcl(String basePath, String bucket) throws S3Exception;
    AccessControlPolicy getObjectAcl(String basePath, String bucket, String key) throws S3Exception;
    void putBucketAcl(String basePath, String bucket, byte[] bytes) throws S3Exception;
    String putObjectAcl(String basePath, String bucket, String key, byte[] bytes) throws S3Exception;

    boolean checkBucketPolicy(String basePath, String bucket, String key, String methodName, S3User s3User)
            throws S3Exception;
    Optional<BucketPolicy> getBucketPolicy(String basePath, String bucket) throws S3Exception;
    Optional<byte[]> getBucketPolicyBytes(String basePath, String bucket) throws S3Exception;
    void putBucketPolicy(String basePath, String bucket, byte[] bytes) throws S3Exception;


    // Actions
    void createBucket(String bucket, String basePath, S3User s3User) throws S3Exception;
    S3Object getObject(ParsedRequest request, String basePath) throws S3Exception;
    S3Object putObject(String bucket, String key, String basePath, byte[] bytes, Map<String, String> metadata,
                       S3User s3User) throws S3Exception;

}
