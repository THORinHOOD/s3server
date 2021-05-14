package com.thorinhood.actions;

import com.thorinhood.BaseTest;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;

import java.io.File;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CreateBucketTest extends BaseTest {

    public CreateBucketTest() {
        super("testS3Java", 9999);
    }

    @Test
    void createBucket() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        CreateBucketRequest request = CreateBucketRequest.builder()
                .bucket("bucket")
                .build();
        CreateBucketResponse response = s3.createBucket(request);
        File bucket = new File(BASE_PATH + File.separatorChar + "bucket");
        Assertions.assertTrue(bucket.exists() && bucket.isDirectory());
        File metaBucketFolder = new File(BASE_PATH + File.separatorChar + ".#bucket");
        Assertions.assertTrue(metaBucketFolder.exists() && metaBucketFolder.isDirectory());
        File aclBucketFile = new File(BASE_PATH + File.separatorChar + ".#bucket" + File.separatorChar +
                "bucket.acl");
        Assertions.assertTrue(aclBucketFile.exists() && aclBucketFile.isFile());

        assertException(HttpResponseStatus.CONFLICT.code(), S3ResponseErrorCodes.BUCKET_ALREADY_OWNED_BY_YOU, () ->
                s3.createBucket(request));

        S3Client s3Client2 = getS3Client(false, ROOT_USER_2.getAccessKey(), ROOT_USER_2.getSecretKey());
        assertException(HttpResponseStatus.CONFLICT.code(), S3ResponseErrorCodes.BUCKET_ALREADY_EXISTS, () ->
                s3Client2.createBucket(request));
    }

}
