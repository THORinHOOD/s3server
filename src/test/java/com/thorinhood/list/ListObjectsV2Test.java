package com.thorinhood.list;

import com.thorinhood.BaseTest;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.List;
import java.util.Map;

public class ListObjectsV2Test extends BaseTest {

    public ListObjectsV2Test() {
        super("testS3Java", 9999);
    }

    @Test
    public void listObjectsV2Simple() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        String content = "hello, s3!!!";
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, null);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, Map.of("key", "value"));
        putObjectRaw(s3, "bucket", "file.txt", content, null);

        List<S3Object> expected = List.of(
                buildS3Object("folder1/folder2/file.txt", ROOT_USER, content),
                buildS3Object("folder1/file.txt", ROOT_USER, content),
                buildS3Object("file.txt", ROOT_USER, content));
        listObjects(s3, "bucket", null, null, null, null, expected);
    }

    @Test
    public void listObjectsV2Unauthorized() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        String content = "hello, s3!!!";
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, null);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, Map.of("key", "value"));
        putObjectRaw(s3, "bucket", "file.txt", content, null);
        List<S3Object> expected = List.of(
                buildS3Object("folder1/folder2/file.txt", ROOT_USER, content),
                buildS3Object("folder1/file.txt", ROOT_USER, content),
                buildS3Object("file.txt", ROOT_USER, content)
        );
        S3Client s3NotAuth = getS3Client(false, NOT_AUTH_ROOT_USER.getAccessKey(),
                NOT_AUTH_ROOT_USER.getSecretKey());
        assertException(HttpResponseStatus.FORBIDDEN.code(), S3ResponseErrorCodes.ACCESS_DENIED, () -> {
            listObjects(s3NotAuth, "bucket", null, null, null, null,
                    expected);
        });
    }

    @Test
    public void listObjectsV2AnotherUser() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        String content = "hello, s3!!!";
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, null);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, Map.of("key", "value"));
        putObjectRaw(s3, "bucket", "file.txt", content, null);

        List<S3Object> expected = List.of(
                buildS3Object("folder1/folder2/file.txt", ROOT_USER, content),
                buildS3Object("folder1/file.txt", ROOT_USER, content),
                buildS3Object("file.txt", ROOT_USER, content)
        );
        S3Client s3Client2 = getS3Client(false, ROOT_USER_2.getAccessKey(), ROOT_USER_2.getSecretKey());
        assertException(HttpResponseStatus.FORBIDDEN.code(), S3ResponseErrorCodes.ACCESS_DENIED, () -> {
            listObjects(s3Client2, "bucket", null, null, null, null,
                    expected);
        });
    }

    @Test
    public void listObjectsV2MaxKeys() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        String content = "hello, s3!!!";
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, null);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, Map.of("key", "value"));
        putObjectRaw(s3, "bucket", "file.txt", content, null);

        List<S3Object> expected = List.of(
                buildS3Object("folder1/file.txt", ROOT_USER, content),
                buildS3Object("file.txt", ROOT_USER, content)
        );
        s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        listObjects(s3, "bucket", 2, null, null, null, expected);
    }

    @Test
    public void listObjectsV2Prefix() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        String content = "hello, s3!!!";
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, null);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, Map.of("key", "value"));
        putObjectRaw(s3, "bucket", "file.txt", content, null);
        putObjectRaw(s3, "bucket", "afile.txt", content, null);
        putObjectRaw(s3, "bucket", "file/dfile.txt", content, null);

        List<S3Object> expected = List.of(
                buildS3Object("file/dfile.txt", ROOT_USER, content),
                buildS3Object("file.txt", ROOT_USER, content)
        );
        s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        listObjects(s3, "bucket", null, "file", null, null, expected);
    }

    @Test
    public void listObjectsV2StartAfter() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        String content = "hello, s3!!!";
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, null);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, Map.of("key", "value"));
        putObjectRaw(s3, "bucket", "file.txt", content, null);
        putObjectRaw(s3, "bucket", "afile.txt", content, null);
        putObjectRaw(s3, "bucket", "file/dfile.txt", content, null);

        List<S3Object> expected = List.of(
                buildS3Object("folder1/folder2/file.txt", ROOT_USER, content),
                buildS3Object("folder1/file.txt", ROOT_USER, content)
        );
        s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        listObjects(s3, "bucket", null, null, "folder1", null, expected);
    }

    @Test
    public void listObjectsV2ContinuousToken() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        String content = "hello, s3!!!";
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, null);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, Map.of("key", "value"));
        putObjectRaw(s3, "bucket", "file.txt", content, null);
        putObjectRaw(s3, "bucket", "afile.txt", content, null);
        putObjectRaw(s3, "bucket", "file/dfile.txt", content, null);

        s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        String nextContinuousToken;

        List<S3Object> expected = List.of(
                buildS3Object("afile.txt", ROOT_USER, content),
                buildS3Object("file.txt", ROOT_USER, content)
        );
        nextContinuousToken = listObjects(s3, "bucket", 2, null, null,
                null, expected);
        Assertions.assertNotNull(nextContinuousToken);

        expected = List.of(
                buildS3Object("file/dfile.txt", ROOT_USER, content),
                buildS3Object("folder1/file.txt", ROOT_USER, content)
        );
        nextContinuousToken = listObjects(s3, "bucket", 2, null, null,
                nextContinuousToken, expected);
        Assertions.assertNotNull(nextContinuousToken);

        expected = List.of(
                buildS3Object("folder1/folder2/file.txt", ROOT_USER, content)
        );
        nextContinuousToken = listObjects(s3, "bucket", 2, null, null,
                nextContinuousToken, expected);
        Assertions.assertNull(nextContinuousToken);
    }

    public String listObjects(S3Client s3, String bucket, Integer maxKeys, String prefix, String startAfter,
                            String continuousToken, List<S3Object> expected) {
        ListObjectsV2Request.Builder request = ListObjectsV2Request.builder()
                .bucket(bucket);
        if (maxKeys != null) {
            request.maxKeys(maxKeys);
        }
        request.prefix(prefix)
               .startAfter(startAfter)
               .continuationToken(continuousToken);
        ListObjectsV2Response response = s3.listObjectsV2(request.build());
        Assertions.assertEquals(maxKeys != null ? maxKeys : 1000, response.maxKeys());
        Assertions.assertEquals(continuousToken, response.continuationToken());
        Assertions.assertEquals(prefix == null ? "" : prefix, response.prefix());
        Assertions.assertEquals(bucket, response.name());
        Assertions.assertEquals(expected.size(), response.contents().size());

        for (S3Object expectedS3Object : expected) {
            Assertions.assertTrue(response.contents().stream()
                .anyMatch(actualS3Object -> equalsS3Objects(expectedS3Object, actualS3Object)));
        }
        return response.nextContinuationToken();
    }

}
