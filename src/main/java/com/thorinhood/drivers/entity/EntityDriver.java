package com.thorinhood.drivers.entity;

import com.thorinhood.data.GetBucketObjectsRequest;
import com.thorinhood.data.S3BucketPath;
import com.thorinhood.data.S3ObjectPath;
import com.thorinhood.data.S3User;
import com.thorinhood.data.s3object.HasMetaData;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.exceptions.S3Exception;
import io.netty.handler.codec.http.HttpHeaders;

import java.util.List;
import java.util.Map;

public interface EntityDriver {

    void createBucket(S3BucketPath s3BucketPath, S3User s3User) throws S3Exception;
    HasMetaData getObject(S3ObjectPath s3ObjectPath, HttpHeaders httpHeaders) throws S3Exception;
    S3Object putObject(S3ObjectPath s3ObjectPath, byte[] bytes, Map<String, String> metadata) throws S3Exception;
    void deleteObject(S3ObjectPath s3ObjectPath) throws S3Exception;
    void deleteBucket(S3BucketPath s3BucketPath) throws S3Exception;
    List<HasMetaData> getBucketObjects(GetBucketObjectsRequest request) throws S3Exception;
    boolean isBucketExists(S3BucketPath s3BucketPath) throws S3Exception;
    boolean isObjectExists(S3ObjectPath s3ObjectPath) throws S3Exception;

}
