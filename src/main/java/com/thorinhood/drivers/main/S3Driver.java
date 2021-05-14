package com.thorinhood.drivers.main;

import com.thorinhood.data.*;
import com.thorinhood.data.acl.AccessControlPolicy;
import com.thorinhood.data.list.eventual.ListBucketResult;
import com.thorinhood.data.list.request.GetBucketObjects;
import com.thorinhood.data.list.request.GetBucketObjectsV2;
import com.thorinhood.data.multipart.Part;
import com.thorinhood.data.policy.BucketPolicy;
import com.thorinhood.data.results.CopyObjectResult;
import com.thorinhood.data.results.GetBucketsResult;
import com.thorinhood.data.list.eventual.ListBucketV2Result;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.exceptions.S3Exception;
import io.netty.handler.codec.http.HttpHeaders;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface S3Driver extends AclPermissionChecker {

    // Permissions
    AccessControlPolicy getBucketAcl(S3FileBucketPath s3FileBucketPath) throws S3Exception;
    AccessControlPolicy getObjectAcl(S3FileObjectPath s3FileObjectPath) throws S3Exception;
    void putBucketAcl(S3FileBucketPath s3FileBucketPath, byte[] bytes) throws S3Exception;
    String putObjectAcl(S3FileObjectPath s3FileObjectPath, byte[] bytes) throws S3Exception;

    Optional<Boolean> checkBucketPolicy(S3FileBucketPath s3FileBucketPath, String key, String methodName, S3User s3User)
            throws S3Exception;
    Optional<BucketPolicy> getBucketPolicy(S3FileBucketPath s3FileBucketPath) throws S3Exception;
    Optional<byte[]> getBucketPolicyBytes(S3FileBucketPath s3FileBucketPath) throws S3Exception;
    void putBucketPolicy(S3FileBucketPath s3FileBucketPath, byte[] bytes) throws S3Exception;
    void isBucketExists(S3FileBucketPath s3FileBucketPath) throws S3Exception;

    // Actions
    void createBucket(S3FileBucketPath s3FileBucketPath, S3User s3User) throws S3Exception;
    S3Object getObject(S3FileObjectPath s3FileObjectPath, HttpHeaders httpHeaders) throws S3Exception;
    S3Object putObject(S3FileObjectPath s3FileObjectPath, byte[] bytes, Map<String, String> metadata, S3User s3User)
            throws S3Exception;
    void deleteObject(S3FileObjectPath s3FileObjectPath) throws S3Exception;
    void deleteBucket(S3FileBucketPath s3FileBucketPath) throws S3Exception;
    CopyObjectResult copyObject(S3FileObjectPath source, S3FileObjectPath target, HttpHeaders httpHeaders,
                                S3User s3User) throws S3Exception;
    S3Object headObject(S3FileObjectPath s3FileObjectPath, HttpHeaders httpHeaders) throws S3Exception;

    // Lists
    ListBucketV2Result getBucketObjectsV2(S3FileBucketPath s3FileBucketPath, GetBucketObjectsV2 getBucketObjectsV2)
            throws S3Exception;
    ListBucketResult getBucketObjects(S3FileBucketPath s3FileBucketPath, GetBucketObjects getBucketObjects)
            throws S3Exception;
    GetBucketsResult getBuckets(S3User s3User) throws S3Exception;

    //Multipart
    String createMultipartUpload(S3FileObjectPath s3FileObjectPath, S3User s3User) throws S3Exception;
    void abortMultipartUpload(S3FileObjectPath s3FileObjectPath, String uploadId) throws S3Exception;
    String putUploadPart(S3FileObjectPath s3FileObjectPath, String uploadId, int partNumber, byte[] bytes) throws S3Exception;
    String completeMultipartUpload(S3FileObjectPath s3FileObjectPath, String uploadId, List<Part> parts, S3User s3User)
            throws S3Exception;

    //Additional
    S3FileObjectPath buildPathToObject(String bucketKeyToObject);
}
