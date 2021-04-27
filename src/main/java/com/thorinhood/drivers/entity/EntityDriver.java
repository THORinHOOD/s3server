package com.thorinhood.drivers.entity;

import com.thorinhood.data.S3User;
import com.thorinhood.data.s3object.HasMetaData;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.exceptions.S3Exception;
import io.netty.handler.codec.http.HttpHeaders;

import java.util.List;
import java.util.Map;

public interface EntityDriver {

    void createBucket(String bucket, S3User s3User) throws S3Exception;
    HasMetaData getObject(String bucket, String key, HttpHeaders httpHeaders) throws S3Exception;
    S3Object putObject(String bucket, String key, byte[] bytes, Map<String, String> metadata) throws S3Exception;
    void deleteObject(String bucket, String key) throws S3Exception;
    void deleteBucket(String bucket) throws S3Exception;
    List<HasMetaData> getBucketObjects(String bucket) throws S3Exception;
    boolean isBucketExists(String bucket) throws S3Exception;
    boolean isObjectExists(String bucket, String key) throws S3Exception;

}
