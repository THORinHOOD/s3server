package com.thorinhood.utils.actions;

import com.thorinhood.utils.BaseTest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

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

        try {
            response = s3.createBucket(request);
            Assertions.fail("BucketAlreadyOwnedByYouException not thrown");
        } catch (BucketAlreadyOwnedByYouException bucketAlreadyExistsException) {
        }

        // TODO OTHER USER must throw BucketAlreadyExistsException

    }

    @Test
    public void incorrectName() {
        S3Client s3Client = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        incorrectNameCheck(s3Client, "aa");
        incorrectNameCheck(s3Client, IntStream.range(0, 64).mapToObj(i -> "a").collect(Collectors.joining()));
        incorrectNameCheck(s3Client, "isBucket?");
        incorrectNameCheck(s3Client, ".#bucket");
        incorrectNameCheck(s3Client, "bucket_$folder$");
    }

    private void incorrectNameCheck(S3Client s3Client, String bucket) {
        try {
            s3Client.createBucket(CreateBucketRequest.builder()
                    .bucket(bucket)
                    .build());
            Assertions.fail("Must be exception");
        } catch (IllegalArgumentException ignored) {
        }
    }
}
