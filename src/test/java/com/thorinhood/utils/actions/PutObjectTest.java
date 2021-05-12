package com.thorinhood.utils.actions;

import com.thorinhood.data.requests.S3ResponseErrorCodes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PutObjectTest extends BaseTest {

    public PutObjectTest() {
        super("testS3Java", 9999);
    }

    @Test
    public void putObjectSimple() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        putObject(s3,
                "bucket",
                null,
                "file.txt",
                createContent(5242880));
    }

    @Test
    public void putObjectCompositeKey() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        putObject(s3,
                "bucket",
                "folder1/folder2",
                "file.txt",
                "hello, s3!!!");
    }

    @Test
    public void putTwoObjectsCompositeKey() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        putObject(s3,
                "bucket",
                "folder1",
                "file.txt",
                "hello, s3!!!");
        putObject(s3,
                "bucket",
                "folder1/folder3",
                "file.txt",
                "hello, s3!!!");
        checkObject("bucket", "folder1", "file.txt", "hello, s3!!!",
                null, true);
    }

    @Test
    public void putTwoObjectsInDifferentBuckets() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        createBucketRaw(s3, "bucket2");
        putObject(s3,
                "bucket",
                "folder1",
                "file.txt",
                "hello, s3!!!");
        putObject(s3,
                "bucket2",
                "folder1",
                "file.txt",
                "hello, s3!!!");
        checkObject("bucket", "folder1", "file.txt", "hello, s3!!!",
                null, true);
        checkObject("bucket2", "folder1", "file.txt", "hello, s3!!!",
                null, true);
    }

    @Test
    public void putChunkedFile() throws Exception {
        S3Client s3 = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        String largeContent = createContent(5242880);
        putObject(s3,
                "bucket",
                "folder1",
                "file.txt",
                largeContent);
        putObject(s3,
                "bucket",
                "folder1",
                "file2.txt",
                "hello, s3, again!!!");
        checkObject("bucket", "folder1", "file.txt", largeContent, null, true);
        checkObject("bucket", "folder1", "file2.txt", "hello, s3, again!!!",
                null, true);
    }

    @Test
    public void putObjectInWrongBucket() throws Exception {
        S3Client s3 = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        try {
            putObject(s3,
                    "bucket2",
                    "folder1",
                    "file.txt",
                    "hello, s3!!!");
            Assertions.fail("NoSuchBucketException not thrown");
        } catch (NoSuchBucketException noSuchBucketException) {
        }
    }

    @Test
    public void putObjectCyrillicSymbolsInKey() throws Exception {
        S3Client s3 = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        putObject(s3,
                "bucket",
                null,
                "файл.txt",
                "привет, s3!!!");
    }

    @Test
    public void putObjectWithMetaData() throws Exception {
        S3Client s3 = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        putObject(s3,
                "bucket",
                null,
                "файл.txt",
                "привет, s3!!!",
                Map.of(
                    "key1", "value1",
                    "key2", "value2"));
    }

    @Test
    public void putObjectWithMetaDataRewrite() throws Exception {
        S3Client s3 = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        putObject(s3,
                "bucket",
                null,
                "файл.txt",
                "привет, s3!!!",
                Map.of(
                        "key1", "value1",
                        "key2", "value2"));
        putObject(s3,
                "bucket",
                null,
                "файл.txt",
                "привет, s3!!!",
                Map.of(
                        "key1", "value1"));
    }

    @Test
    public void putObjectUnregisterUser() throws Exception {
        S3Client s3 = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        s3 = getS3Client(true, NOT_AUTH_ROOT_USER.getAccessKey(), NOT_AUTH_ROOT_USER.getSecretKey());
        try {
            putObject(s3,
                    "bucket",
                    null,
                    "файл.txt",
                    "привет, s3!!!",
                    Map.of(
                            "key1", "value1",
                            "key2", "value2"));
            Assertions.fail("Access denied exception not thrown");
        } catch (S3Exception exception) {
            Assertions.assertEquals(exception.awsErrorDetails().errorCode(), S3ResponseErrorCodes.ACCESS_DENIED);
            Assertions.assertEquals(exception.awsErrorDetails().errorMessage(), "Access denied");
        }
    }

    @Test
    public void putObjectAnotherUser() throws Exception {
        S3Client s3 = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        s3 = getS3Client(true, ROOT_USER_2.getAccessKey(), ROOT_USER_2.getSecretKey());
        try {
            putObject(s3,
                    "bucket",
                    null,
                    "файл.txt",
                    "привет, s3!!!",
                    Map.of(
                            "key1", "value1",
                            "key2", "value2"));
            Assertions.fail("Access denied exception not thrown");
        } catch (S3Exception exception) {
            Assertions.assertEquals(exception.awsErrorDetails().errorCode(), S3ResponseErrorCodes.ACCESS_DENIED);
            Assertions.assertEquals(exception.awsErrorDetails().errorMessage(), "Access denied");
        }
    }

    @Test
    public void putObjectSeveralRequests() throws Exception {
        S3Client s3 = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        S3AsyncClient s3Async = getS3AsyncClient(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        String firstContent = createContent(5242880);
        String secondContent = createContent(5242880 * 2);
        List<String> contents = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            contents.add(i % 2 == 0 ? firstContent : secondContent);
        }
        List<CompletableFuture<PutObjectResponse>> futureList = putObjectAsync(s3Async, "bucket",
                "folder1/folder2", "file.txt", contents, null);
        checkPutObjectAsync("bucket", "folder1/folder2", "file.txt", futureList,
                contents, null);
    }

    private void putObject(S3Client s3Client, String bucket, String keyWithoutName, String fileName, String content)
            throws IOException {
        putObject(s3Client, bucket, keyWithoutName, fileName, content, null);
    }

    private void putObject(S3Client s3Client, String bucket, String keyWithoutName, String fileName, String content,
                           Map<String, String> metadata) throws IOException {
        PutObjectRequest.Builder request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(buildKey(keyWithoutName, fileName));
        if (metadata != null) {
            request.metadata(metadata);
        }
        PutObjectResponse response = s3Client.putObject(request.build(), RequestBody.fromString(content));
        Assertions.assertEquals(response.eTag(), "\"" + calcETag(content) + "\"");
        checkObject(bucket, keyWithoutName, fileName, content, metadata, true);
    }

}
