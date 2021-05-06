package com.thorinhood.utils.actions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;

import java.io.File;

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

}
