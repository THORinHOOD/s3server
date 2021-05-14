package com.thorinhood.utils.actions;

import com.thorinhood.data.requests.S3ResponseErrorCodes;
import com.thorinhood.utils.BaseTest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;

import java.io.File;

public class DeleteBucketTest extends BaseTest {

    public DeleteBucketTest() {
        super("testS3Java", 9999);
    }

    @Test
    public void simpleDeleteBucket() {
        S3Client s3Client = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3Client, "bucket");
        createBucketRaw(s3Client, "bucket2");
        s3Client.deleteBucket(DeleteBucketRequest.builder()
                .bucket("bucket")
                .build());
        ListBucketsResponse response = s3Client.listBuckets();
        Assertions.assertEquals(1, response.buckets().size());
        Assertions.assertEquals("bucket2", response.buckets().get(0).name());
        File bucket = new File(BASE_PATH + File.separatorChar + "bucket");
        Assertions.assertFalse(bucket.exists());
        File bucket2 = new File(BASE_PATH + File.separatorChar + "bucket2");
        Assertions.assertTrue(bucket2.exists() && bucket2.isDirectory());
        assertException(HttpResponseStatus.FORBIDDEN.code(), S3ResponseErrorCodes.ACCESS_DENIED, () -> {
            getS3Client(false, ROOT_USER_2.getAccessKey(), ROOT_USER_2.getSecretKey())
                    .deleteBucket(DeleteBucketRequest.builder()
                            .bucket("bucket2")
                            .build());
        });
    }

}
