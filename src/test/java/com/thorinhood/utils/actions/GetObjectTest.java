package com.thorinhood.utils.actions;

import com.thorinhood.data.requests.S3ResponseErrorCodes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class GetObjectTest extends BaseTest {

    public GetObjectTest() {
        super("testS3Java", 9999);
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

    @Test
    public void getObjectWithHeaders() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        String content = "hello, s3!!!";
        Map<String, String> metadata = Map.of("key", "value");
        createBucketRaw("bucket", s3);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, metadata);

        getObject(s3, "bucket", "folder1/file.txt", content, metadata, calcETag(content), null);
        getObject(s3, "bucket", "folder1/file.txt", content, metadata, null, "aaa");

        try {
            getObject(s3, "bucket", "folder1/file.txt", content, metadata, "aaa", null);
            Assertions.fail("Precondition exception is not thrown");
        } catch (S3Exception exception) {
            Assertions.assertEquals(exception.awsErrorDetails().errorCode(), S3ResponseErrorCodes.PRECONDITION_FAILED);
            Assertions.assertEquals(exception.awsErrorDetails().errorMessage(),
                    "At least one of the pre-conditions you specified did not hold");
        }

        try {
            getObject(s3, "bucket", "folder1/file.txt", content, metadata, null, calcETag(content));
            Assertions.fail("Precondition exception is not thrown");
        } catch (S3Exception exception) {
        }
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
        createBucketRaw("bucket", s3);
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, metadata);

        getObjectAsync(s3Async, "bucket", "folder1/folder2/file.txt", content, metadata, null,
                null, 200);
    }

    private void getObject(S3Client s3, String bucket, String key, String content, Map<String, String> metadata)
            throws Exception {
        getObject(s3, bucket, key, content, metadata, null, null);
    }

    private void getObject(S3Client s3, String bucket, String key, String content, Map<String, String> metadata,
                           String ifMatch, String ifNoneMatch) throws Exception {
        GetObjectRequest.Builder request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key);
        if (ifMatch != null) {
            request.ifMatch(ifMatch);
        }
        if (ifNoneMatch != null) {
            request.ifNoneMatch(ifNoneMatch);
        }

        ResponseBytes<GetObjectResponse> response = s3.getObject(request.build(), ResponseTransformer.toBytes());
        check(content, metadata, response);
    }

    private void getObjectAsync(S3AsyncClient s3, String bucket, String key, String content,
                                Map<String, String> metadata, String ifMatch, String ifNoneMatch,
                                int requestsCount) throws Exception {
        GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key);
        if (ifMatch != null) {
            requestBuilder.ifMatch(ifMatch);
        }
        if (ifNoneMatch != null) {
            requestBuilder.ifNoneMatch(ifNoneMatch);
        }
        GetObjectRequest request = requestBuilder.build();
        List<CompletableFuture<ResponseBytes<GetObjectResponse>>> futureList = new ArrayList<>();
        for (int i = 0; i < requestsCount; i++) {
            futureList.add(s3.getObject(request, AsyncResponseTransformer.toBytes()));
        }
        for (int i = 0; i < requestsCount; i++) {
            check(content, metadata, futureList.get(i).get());
        }
    }

    private void check(String expectedContent, Map<String, String> expectedMetadata,
                       ResponseBytes<GetObjectResponse> response) {
        Assertions.assertEquals(expectedContent.getBytes().length, response.response().contentLength());
        Assertions.assertEquals(calcETag(expectedContent), response.response().eTag());
        if (expectedMetadata != null) {
            assertMaps(expectedMetadata, response.response().metadata());
        } else {
            Assertions.assertTrue(response.response().metadata() == null ||
                    response.response().metadata().isEmpty());
        }
        Assertions.assertEquals(expectedContent, new String(response.asByteArray()));
    }


}
