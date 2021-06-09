package com.thorinhood.actions;

import com.thorinhood.BaseTest;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class GetObjectTest extends BaseTest {

    public GetObjectTest() {
        super("testS3Java", 9999);
    }

    @Test
    public void getObjectSimple() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        String content = "hello, s3!!!";
        Map<String, String> metadata = Map.of(
                "key", "value",
                "key1", "value1");
        createBucketRaw(s3, "bucket");
        putObjectRaw(s3, "bucket", "file.txt", content, metadata);
        getObject(s3, "bucket", "file.txt", content, metadata);
    }

    @Test
    public void getObjectCompositeKey() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        String content = "hello, s3!!!";
        Map<String, String> metadata = Map.of("key", "value");
        createBucketRaw(s3, "bucket");
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, metadata);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, metadata);
        getObject(s3, "bucket", "folder1/file.txt", content, metadata);
        getObject(s3, "bucket", "folder1/folder2/file.txt", content, metadata);
    }

    @Test
    public void getObjectUnregisterUser() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        String content = "hello, s3!!!";
        Map<String, String> metadata = Map.of("key", "value");
        createBucketRaw(s3, "bucket");
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, metadata);
        S3Client s3ClientNotAuth = getS3Client(false, NOT_AUTH_ROOT_USER.getAccessKey(),
                NOT_AUTH_ROOT_USER.getSecretKey());
        assertException(HttpResponseStatus.FORBIDDEN.code(), S3ResponseErrorCodes.ACCESS_DENIED, () -> {
            getObject(s3ClientNotAuth, "bucket", "folder1/file.txt", content, metadata);
        });
    }

    @Test
    public void getObjectAnotherUser() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        String content = "hello, s3!!!";
        Map<String, String> metadata = Map.of("key", "value");
        createBucketRaw(s3, "bucket");
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, metadata);
        S3Client s3Client2 = getS3Client(false, ROOT_USER_2.getAccessKey(), ROOT_USER_2.getSecretKey());
        assertException(HttpResponseStatus.FORBIDDEN.code(), S3ResponseErrorCodes.ACCESS_DENIED, () -> {
            getObject(s3Client2, "bucket", "folder1/file.txt", content, metadata);
        });
    }

    @Test
    public void getObjectWithHeaders() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        String content = "hello, s3!!!";
        Map<String, String> metadata = Map.of("key", "value");
        createBucketRaw(s3, "bucket");
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, metadata);

        getObject(s3, "bucket", "folder1/file.txt", content, metadata, calcETag(content), null);
        getObject(s3, "bucket", "folder1/file.txt", content, metadata, null, "aaa");

        assertException(HttpResponseStatus.PRECONDITION_FAILED.code(), S3ResponseErrorCodes.PRECONDITION_FAILED, () -> {
            getObject(s3, "bucket", "folder1/file.txt", content, metadata, "aaa", null);
        });

        assertException(HttpResponseStatus.PRECONDITION_FAILED.code(), S3ResponseErrorCodes.PRECONDITION_FAILED, () -> {
            getObject(s3, "bucket", "folder1/file.txt", content, metadata, null, calcETag(content));
        });
    }

    @Test
    public void getObjectSeveralRequests() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        S3AsyncClient s3Async = getS3AsyncClient(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        String content = createContent(5242880);
        Map<String, String> metadata = Map.of(
                "key", "value",
                "key3", "value3",
                "key12", "value12"
        );
        createBucketRaw(s3, "bucket");
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, metadata);

        List<CompletableFuture<ResponseBytes<GetObjectResponse>>> futureList = getObjectAsync(s3Async, "bucket",
                "folder1/folder2/file.txt", null, null, 200);
        for (CompletableFuture<ResponseBytes<GetObjectResponse>> responseBytesCompletableFuture : futureList) {
            checkGetObject(content, metadata, responseBytesCompletableFuture.get());
        }
    }

}
