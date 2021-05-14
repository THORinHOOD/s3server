package com.thorinhood.utils.list;

import com.thorinhood.data.requests.S3ResponseErrorCodes;
import com.thorinhood.utils.BaseTest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;

public class ListBucketsTest extends BaseTest {
    public ListBucketsTest() {
        super("testS3Java", 9999);
    }

    @Test
    public void simpleListBuckets() {
        S3Client s3Client = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3Client, "bucket");
        createBucketRaw(s3Client, "bucket2");
        S3Client s3Client2 = getS3Client(false, ROOT_USER_2.getAccessKey(), ROOT_USER_2.getSecretKey());
        createBucketRaw(s3Client2, "bucket3");
        ListBucketsResponse response = s3Client.listBuckets();
        Assertions.assertEquals(2, response.buckets().size());
        for (Bucket bucket : response.buckets()) {
            Assertions.assertTrue(bucket.name().equals("bucket") || bucket.name().equals("bucket2"));
        }
        response = s3Client2.listBuckets();
        Assertions.assertEquals(1, response.buckets().size());
        Assertions.assertEquals("bucket3", response.buckets().get(0).name());
        S3Client s3NotAuth = getS3Client(false, NOT_AUTH_ROOT_USER.getAccessKey(),
                NOT_AUTH_ROOT_USER.getSecretKey());
        assertException(HttpResponseStatus.FORBIDDEN.code(), S3ResponseErrorCodes.ACCESS_DENIED,
                s3NotAuth::listBuckets);
    }
}
