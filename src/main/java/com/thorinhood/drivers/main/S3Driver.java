package com.thorinhood.drivers.main;

import com.thorinhood.data.S3Content;
import com.thorinhood.data.S3User;
import com.thorinhood.data.acl.AccessControlPolicy;
import com.thorinhood.data.policy.BucketPolicy;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.exceptions.S3Exception;
import io.netty.handler.codec.http.HttpHeaders;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface S3Driver extends AclPermissionChecker {

    // Permissions
    AccessControlPolicy getBucketAcl(String bucket) throws S3Exception;
    AccessControlPolicy getObjectAcl(String bucket, String key) throws S3Exception;
    void putBucketAcl(String bucket, byte[] bytes) throws S3Exception;
    String putObjectAcl(String bucket, String key, byte[] bytes) throws S3Exception;

    boolean checkBucketPolicy(String bucket, String key, String methodName, S3User s3User) throws S3Exception;
    Optional<BucketPolicy> getBucketPolicy(String bucket) throws S3Exception;
    Optional<byte[]> getBucketPolicyBytes(String bucket) throws S3Exception;
    void putBucketPolicy(String bucket, byte[] bytes) throws S3Exception;
    void isBucketExists(String bucket) throws S3Exception;
    void isObjectExists(String bucket, String key) throws S3Exception;

    // Actions
    void createBucket(String bucket, S3User s3User) throws S3Exception;
    S3Object getObject(String bucket, String key, HttpHeaders httpHeaders) throws S3Exception;
    S3Object putObject(String bucket, String key, byte[] bytes, Map<String, String> metadata, S3User s3User)
            throws S3Exception;
    void deleteObject(String bucket, String key) throws S3Exception;
    void deleteBucket(String bucket) throws S3Exception;

    // Lists
    List<S3Content> getBucketObjects(String bucket) throws S3Exception;
}
