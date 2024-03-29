package com.thorinhood.actions;

import com.thorinhood.BaseTest;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import java.io.IOException;
import java.util.Map;

public class DeleteObjectTest extends BaseTest {

    public DeleteObjectTest() {
        super("testS3Java", 9999);
    }

    @Test
    public void deleteObjectSimple() throws IOException {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        putObjectRaw(s3, "bucket", "file.txt", "hello, s3!!!", Map.of("key", "value"));
        checkObject("bucket", null, "file.txt", "hello, s3!!!",
                Map.of("key", "value"), true);
        deleteObject(s3, "bucket", "file.txt");
        checkObjectNotExists("bucket", null, "file.txt");
    }

    @Test
    public void deleteObjectCompositeKey() throws IOException {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", "hello, s3!!!", null);
        checkObject("bucket", "folder1/folder2", "file.txt", "hello, s3!!!",
                null, true);
        deleteObject(s3, "bucket", "folder1/folder2/file.txt");
        checkObjectNotExists("bucket", "folder1/folder2", "file.txt");
    }

    @Test
    public void deleteObjectSimilarFolder() throws IOException {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        putObjectRaw(s3, "bucket", "folder1/file.txt", "hello, s3!!!", null);
        putObjectRaw(s3, "bucket", "folder1/file2.txt", "hello, s3!!!", null);
        checkObject("bucket", "folder1", "file.txt", "hello, s3!!!",
                null, true);
        checkObject("bucket", "folder1", "file2.txt", "hello, s3!!!",
                null, true);
        deleteObject(s3, "bucket", "folder1/file2.txt");
        checkObjectNotExists("bucket", "folder1", "file2.txt");
        checkObject("bucket", "folder1", "file.txt", "hello, s3!!!",
                null);
    }

    @Test
    public void deleteObjectSimilarNames() throws IOException {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        putObjectRaw(s3, "bucket", "folder1/file.txt", "hello, s3!!!", null);
        putObjectRaw(s3, "bucket", "folder2/file.txt", "hello, s3!!!", null);
        checkObject("bucket", "folder1", "file.txt", "hello, s3!!!",
                null);
        checkObject("bucket", "folder2", "file.txt", "hello, s3!!!",
                null);
        deleteObject(s3, "bucket", "folder1/file.txt");
        checkObjectNotExists("bucket", "folder1", "file.txt");
        checkObject("bucket", "folder2", "file.txt", "hello, s3!!!",
                null);
    }

    @Test
    public void deleteObjectDifferentBuckets() throws IOException {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        createBucketRaw(s3, "bucket2");
        putObjectRaw(s3, "bucket", "folder1/file.txt", "hello, s3!!!", null);
        putObjectRaw(s3, "bucket2", "folder1/file.txt", "hello, s3!!!", null);
        checkObject("bucket", "folder1", "file.txt", "hello, s3!!!",
                null);
        checkObject("bucket2", "folder1", "file.txt", "hello, s3!!!",
                null);
        deleteObject(s3, "bucket2", "folder1/file.txt");
        checkObjectNotExists("bucket2", "folder1", "file.txt");
        checkObject("bucket", "folder1", "file.txt", "hello, s3!!!",
                null);
    }

    @Test
    public void deleteObjectUnauthorizedUser() throws IOException {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        putObjectRaw(s3, "bucket", "folder1/file.txt", "hello, s3!!!", null);
        checkObject("bucket", "folder1", "file.txt", "hello, s3!!!",
                null);
        S3Client s3NotAuth = getS3Client(false, NOT_AUTH_ROOT_USER.getAccessKey(),
                NOT_AUTH_ROOT_USER.getSecretKey());
        assertException(HttpResponseStatus.FORBIDDEN.code(), S3ResponseErrorCodes.ACCESS_DENIED, () -> {
            deleteObject(s3NotAuth, "bucket", "folder1/file.txt");
        });
    }

    @Test
    public void deleteObjectOtherUser() throws IOException {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        putObjectRaw(s3, "bucket", "folder1/file.txt", "hello, s3!!!", null);
        checkObject("bucket", "folder1", "file.txt", "hello, s3!!!",
                null);
        S3Client s3Client2 = getS3Client(false, ROOT_USER_2.getAccessKey(), ROOT_USER_2.getSecretKey());
        assertException(HttpResponseStatus.FORBIDDEN.code(), S3ResponseErrorCodes.ACCESS_DENIED, () -> {
            deleteObject(s3Client2, "bucket", "folder1/file.txt");
        });
    }

    private void deleteObject(S3Client s3, String bucket, String key) {
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .key(key)
                .bucket(bucket)
                .build();
        s3.deleteObject(request);
    }

}
