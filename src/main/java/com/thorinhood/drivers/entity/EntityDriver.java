package com.thorinhood.drivers.entity;

import com.thorinhood.data.list.raw.ListBucketResultRaw;
import com.thorinhood.data.list.request.GetBucketObjects;
import com.thorinhood.data.list.request.GetBucketObjectsV2;
import com.thorinhood.data.S3FileBucketPath;
import com.thorinhood.data.S3FileObjectPath;
import com.thorinhood.data.S3User;
import com.thorinhood.data.multipart.Part;
import com.thorinhood.data.list.raw.ListBucketV2ResultRaw;
import com.thorinhood.data.s3object.HasMetaData;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.utils.Pair;
import io.netty.handler.codec.http.HttpHeaders;

import java.util.List;
import java.util.Map;

public interface EntityDriver {
    void createBucket(S3FileBucketPath s3FileBucketPath, S3User s3User) throws S3Exception;
    HasMetaData getObject(S3FileObjectPath s3FileObjectPath, String eTag, HttpHeaders httpHeaders, boolean isCopyRead)
            throws S3Exception;
    HasMetaData headObject(S3FileObjectPath s3FileObjectPath, String eTag, HttpHeaders httpHeaders) throws S3Exception;
    S3Object putObject(S3FileObjectPath s3FileObjectPath, byte[] bytes, Map<String, String> metadata)
            throws S3Exception;
    void deleteObject(S3FileObjectPath s3FileObjectPath) throws S3Exception;
    void deleteBucket(S3FileBucketPath s3FileBucketPath) throws S3Exception;
    ListBucketV2ResultRaw getBucketObjectsV2(GetBucketObjectsV2 getBucketObjectsV2) throws S3Exception;
    ListBucketResultRaw getBucketObjects(GetBucketObjects getBucketObjects) throws S3Exception;
    List<Pair<S3FileBucketPath, String>> getBuckets(S3User s3User) throws S3Exception;
    void createMultipartUpload(S3FileObjectPath s3FileObjectPath, String uploadId) throws S3Exception;
    void abortMultipartUpload(S3FileObjectPath s3FileObjectPath, String uploadId) throws S3Exception;
    String putUploadPart(S3FileObjectPath s3FileObjectPath, String uploadId, int partNumber, byte[] bytes)
            throws S3Exception;
    String completeMultipartUpload(S3FileObjectPath s3FileObjectPath, String uploadId, List<Part> parts)
            throws S3Exception;
}
