package com.thorinhood.utils.actions;

import com.thorinhood.data.S3ResponseErrorCodes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.Map;

public class GetObjectTest extends BaseTest {

    public GetObjectTest() {
        super("/home/thorinhood/testS3Java", 9999);
    }

    @Test
    public void getObjectSimple() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        String content = "hello, s3!!!";
        Map<String, String> metadata = Map.of(
                "key", "value",
                "key1", "value1");
        createBucketRaw("bucket", s3);
        putObjectRaw(s3, "bucket", "file.txt", content, metadata);
        getObject(s3, "bucket", "file.txt", content, metadata);
    }

    @Test
    public void getObjectCompositeKey() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        String content = "hello, s3!!!";
        Map<String, String> metadata = Map.of("key", "value");
        createBucketRaw("bucket", s3);
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, metadata);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, metadata);
        getObject(s3, "bucket", "folder1/file.txt", content, metadata);
        getObject(s3, "bucket", "folder1/folder2/file.txt", content, metadata);
    }

    @Test
    public void getObjectUnregisterUser() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        String content = "hello, s3!!!";
        Map<String, String> metadata = Map.of("key", "value");
        createBucketRaw("bucket", s3);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, metadata);
        s3 = getS3Client(false, NOT_AUTH_ROOT_USER.getAccessKey(), NOT_AUTH_ROOT_USER.getSecretKey());
        try {
            getObject(s3, "bucket", "folder1/file.txt", content, metadata);
            Assertions.fail("Access denied exception not thrown");
        } catch (S3Exception exception) {
            Assertions.assertEquals(exception.awsErrorDetails().errorCode(), S3ResponseErrorCodes.ACCESS_DENIED);
            Assertions.assertEquals(exception.awsErrorDetails().errorMessage(), "Access denied");
        }
    }

    @Test
    public void getObjectAnotherUser() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        String content = "hello, s3!!!";
        Map<String, String> metadata = Map.of("key", "value");
        createBucketRaw("bucket", s3);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, metadata);
        s3 = getS3Client(false, ROOT_USER_2.getAccessKey(), ROOT_USER_2.getSecretKey());
        try {
            getObject(s3, "bucket", "folder1/file.txt", content, metadata);
            Assertions.fail("Access denied exception not thrown");
        } catch (S3Exception exception) {
            Assertions.assertEquals(exception.awsErrorDetails().errorCode(), S3ResponseErrorCodes.ACCESS_DENIED);
            Assertions.assertEquals(exception.awsErrorDetails().errorMessage(), "Access denied");
        }
    }

    private void getObject(S3Client s3, String bucket, String key, String content, Map<String, String> metadata)
            throws Exception {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        ResponseInputStream<GetObjectResponse> response = s3.getObject(request);
        Assertions.assertEquals(content.getBytes().length, response.response().contentLength());
        Assertions.assertEquals(calcETag(content), response.response().eTag());
        if (metadata != null) {
            assertMaps(metadata, response.response().metadata());
        } else {
            Assertions.assertTrue(response.response().metadata() == null ||
                    response.response().metadata().isEmpty());
        }
        Assertions.assertEquals(content, new String(response.readAllBytes()));
    }

}
