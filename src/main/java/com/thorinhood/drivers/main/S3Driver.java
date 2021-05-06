package com.thorinhood.drivers.main;

import com.thorinhood.data.*;
import com.thorinhood.data.acl.AccessControlPolicy;
import com.thorinhood.data.multipart.Part;
import com.thorinhood.data.policy.BucketPolicy;
import com.thorinhood.data.results.GetBucketsResult;
import com.thorinhood.data.results.ListBucketResult;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.exceptions.S3Exception;
import io.netty.handler.codec.http.HttpHeaders;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface S3Driver extends AclPermissionChecker {

    // Permissions
    AccessControlPolicy getBucketAcl(S3BucketPath s3BucketPath) throws S3Exception;
    AccessControlPolicy getObjectAcl(S3ObjectPath s3ObjectPath) throws S3Exception;
    void putBucketAcl(S3BucketPath s3BucketPath, byte[] bytes) throws S3Exception;
    String putObjectAcl(S3ObjectPath s3ObjectPath, byte[] bytes) throws S3Exception;

    boolean checkBucketPolicy(S3BucketPath s3BucketPath, String key, String methodName, S3User s3User) throws S3Exception;
    Optional<BucketPolicy> getBucketPolicy(S3BucketPath s3BucketPath) throws S3Exception;
    Optional<byte[]> getBucketPolicyBytes(S3BucketPath s3BucketPath) throws S3Exception;
    void putBucketPolicy(S3BucketPath s3BucketPath, byte[] bytes) throws S3Exception;
    void isBucketExists(S3BucketPath s3BucketPath) throws S3Exception;
    void isObjectExists(S3ObjectPath s3ObjectPath) throws S3Exception;

    // Actions
    void createBucket(S3BucketPath s3BucketPath, S3User s3User) throws S3Exception;
    S3Object getObject(S3ObjectPath s3ObjectPath, HttpHeaders httpHeaders) throws S3Exception;
    S3Object putObject(S3ObjectPath s3ObjectPath, byte[] bytes, Map<String, String> metadata, S3User s3User)
            throws S3Exception;
    void deleteObject(S3ObjectPath s3ObjectPath) throws S3Exception;
    void deleteBucket(S3BucketPath s3BucketPath) throws S3Exception;

    // Lists
    ListBucketResult getBucketObjects(GetBucketObjects request) throws S3Exception;
    GetBucketsResult getBuckets(S3User s3User) throws S3Exception;

    //Multipart
    String createMultipartUpload(S3ObjectPath s3ObjectPath) throws S3Exception;
    void abortMultipartUpload(S3ObjectPath s3ObjectPath, String uploadId) throws S3Exception;
    String putUploadPart(S3ObjectPath s3ObjectPath, String uploadId, int partNumber, byte[] bytes) throws S3Exception;
    String completeMultipartUpload(S3ObjectPath s3ObjectPath, String uploadId, List<Part> parts, S3User s3User)
            throws S3Exception;
}
