package com.thorinhood.utils.actions;

import com.thorinhood.data.S3ResponseErrorCodes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.List;
import java.util.Map;

public class ListObjectsTest extends BaseTest {

    public ListObjectsTest() {
        super("/home/thorinhood/testS3Java", 9999);
    }

    @Test
    public void listObjectsSimple() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw("bucket", s3);
        String content = "hello, s3!!!";
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, null);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, Map.of("key", "value"));
        putObjectRaw(s3, "bucket", "file.txt", content, null);

        String eTag = calcETag(content);
        long size = content.getBytes().length;
        List<S3Object> expected = List.of(
                S3Object.builder()
                    .eTag(eTag)
                    .key("folder1/folder2/file.txt")
                    .owner(Owner.builder()
                            .id(ROOT_USER.getCanonicalUserId())
                            .displayName(ROOT_USER.getAccountName())
                            .build())
                    .size(size)
                    .build(),
                S3Object.builder()
                    .eTag(eTag)
                    .key("folder1/file.txt")
                    .owner(Owner.builder()
                            .id(ROOT_USER.getCanonicalUserId())
                            .displayName(ROOT_USER.getAccountName())
                            .build())
                    .size(size)
                    .build(),
                S3Object.builder()
                        .eTag(eTag)
                        .key("file.txt")
                        .owner(Owner.builder()
                                .id(ROOT_USER.getCanonicalUserId())
                                .displayName(ROOT_USER.getAccountName())
                                .build())
                        .size(size)
                        .build()
                );
        listObjects(s3, "bucket", null, null, expected);
    }

    @Test
    public void listObjectsUnauthorized() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw("bucket", s3);
        String content = "hello, s3!!!";
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, null);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, Map.of("key", "value"));
        putObjectRaw(s3, "bucket", "file.txt", content, null);

        String eTag = calcETag(content);
        long size = content.getBytes().length;
        List<S3Object> expected = List.of(
                S3Object.builder()
                        .eTag(eTag)
                        .key("folder1/folder2/file.txt")
                        .owner(Owner.builder()
                                .id(ROOT_USER.getCanonicalUserId())
                                .displayName(ROOT_USER.getAccountName())
                                .build())
                        .size(size)
                        .build(),
                S3Object.builder()
                        .eTag(eTag)
                        .key("folder1/file.txt")
                        .owner(Owner.builder()
                                .id(ROOT_USER.getCanonicalUserId())
                                .displayName(ROOT_USER.getAccountName())
                                .build())
                        .size(size)
                        .build(),
                S3Object.builder()
                        .eTag(eTag)
                        .key("file.txt")
                        .owner(Owner.builder()
                                .id(ROOT_USER.getCanonicalUserId())
                                .displayName(ROOT_USER.getAccountName())
                                .build())
                        .size(size)
                        .build()
        );
        s3 = getS3Client(false, NOT_AUTH_ROOT_USER.getAccessKey(), NOT_AUTH_ROOT_USER.getSecretKey());
        try {
            listObjects(s3, "bucket", null, null, expected);
            Assertions.fail("Access denied exception not thrown");
        } catch (S3Exception exception) {
            Assertions.assertEquals(exception.awsErrorDetails().errorCode(), S3ResponseErrorCodes.ACCESS_DENIED);
            Assertions.assertEquals(exception.awsErrorDetails().errorMessage(), "Access denied");
        }
    }

    @Test
    public void listObjectsAnotherUser() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw("bucket", s3);
        String content = "hello, s3!!!";
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, null);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, Map.of("key", "value"));
        putObjectRaw(s3, "bucket", "file.txt", content, null);

        String eTag = calcETag(content);
        long size = content.getBytes().length;
        List<S3Object> expected = List.of(
                S3Object.builder()
                        .eTag(eTag)
                        .key("folder1/folder2/file.txt")
                        .owner(Owner.builder()
                                .id(ROOT_USER.getCanonicalUserId())
                                .displayName(ROOT_USER.getAccountName())
                                .build())
                        .size(size)
                        .build(),
                S3Object.builder()
                        .eTag(eTag)
                        .key("folder1/file.txt")
                        .owner(Owner.builder()
                                .id(ROOT_USER.getCanonicalUserId())
                                .displayName(ROOT_USER.getAccountName())
                                .build())
                        .size(size)
                        .build(),
                S3Object.builder()
                        .eTag(eTag)
                        .key("file.txt")
                        .owner(Owner.builder()
                                .id(ROOT_USER.getCanonicalUserId())
                                .displayName(ROOT_USER.getAccountName())
                                .build())
                        .size(size)
                        .build()
        );
        s3 = getS3Client(false, ROOT_USER_2.getAccessKey(), ROOT_USER_2.getSecretKey());
        try {
            listObjects(s3, "bucket", null, null, expected);
            Assertions.fail("Access denied exception not thrown");
        } catch (S3Exception exception) {
            Assertions.assertEquals(exception.awsErrorDetails().errorCode(), S3ResponseErrorCodes.ACCESS_DENIED);
            Assertions.assertEquals(exception.awsErrorDetails().errorMessage(), "Access denied");
        }
    }

    @Test
    public void listObjectsMaxKeys() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw("bucket", s3);
        String content = "hello, s3!!!";
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, null);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, Map.of("key", "value"));
        putObjectRaw(s3, "bucket", "file.txt", content, null);

        String eTag = calcETag(content);
        long size = content.getBytes().length;
        List<S3Object> expected = List.of(
                S3Object.builder()
                        .eTag(eTag)
                        .key("folder1/file.txt")
                        .owner(Owner.builder()
                                .id(ROOT_USER.getCanonicalUserId())
                                .displayName(ROOT_USER.getAccountName())
                                .build())
                        .size(size)
                        .build(),
                S3Object.builder()
                        .eTag(eTag)
                        .key("file.txt")
                        .owner(Owner.builder()
                                .id(ROOT_USER.getCanonicalUserId())
                                .displayName(ROOT_USER.getAccountName())
                                .build())
                        .size(size)
                        .build()
        );
        s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        listObjects(s3, "bucket", 2, null, expected);
    }

    @Test
    public void listObjectsPrefix() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw("bucket", s3);
        String content = "hello, s3!!!";
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, null);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, Map.of("key", "value"));
        putObjectRaw(s3, "bucket", "file.txt", content, null);
        putObjectRaw(s3, "bucket", "afile.txt", content, null);
        putObjectRaw(s3, "bucket", "file/dfile.txt", content, null);

        String eTag = calcETag(content);
        long size = content.getBytes().length;
        List<S3Object> expected = List.of(
                S3Object.builder()
                        .eTag(eTag)
                        .key("file/dfile.txt")
                        .owner(Owner.builder()
                                .id(ROOT_USER.getCanonicalUserId())
                                .displayName(ROOT_USER.getAccountName())
                                .build())
                        .size(size)
                        .build(),
                S3Object.builder()
                        .eTag(eTag)
                        .key("file.txt")
                        .owner(Owner.builder()
                                .id(ROOT_USER.getCanonicalUserId())
                                .displayName(ROOT_USER.getAccountName())
                                .build())
                        .size(size)
                        .build()
        );
        s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        listObjects(s3, "bucket", null, "file", expected);
    }

    public void listObjects(S3Client s3, String bucket, Integer maxKeys, String prefix, List<S3Object> expected) {
        ListObjectsRequest.Builder request = ListObjectsRequest.builder()
                .bucket(bucket);
        if (maxKeys != null) {
            request.maxKeys(maxKeys);
        }
        request.prefix(prefix);
        ListObjectsResponse response = s3.listObjects(request.build());
        Assertions.assertEquals(maxKeys != null ? maxKeys : 1000, response.maxKeys());
        Assertions.assertEquals(prefix, response.prefix());
        Assertions.assertEquals(bucket, response.name());
        Assertions.assertEquals(expected.size(), response.contents().size());

        for (S3Object expectedS3Object : expected) {
            Assertions.assertTrue(response.contents().stream()
                .anyMatch(actualS3Object -> equalsS3Objects(expectedS3Object, actualS3Object)));
        }
    }

    private boolean equalsS3Objects(S3Object expected, S3Object actual) {
        return expected.key().equals(actual.key()) &&
                expected.size().equals(actual.size()) &&
                expected.eTag().equals(actual.eTag()) &&
                expected.owner().displayName().equals(actual.owner().displayName()) &&
                expected.owner().id().equals(actual.owner().id());
    }

}
