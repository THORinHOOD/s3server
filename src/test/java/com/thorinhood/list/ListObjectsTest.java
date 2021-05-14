package com.thorinhood.list;

import com.thorinhood.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.List;
import java.util.Map;

public class ListObjectsTest extends BaseTest {

    public ListObjectsTest() {
        super("testS3Java", 9999);
    }

    @Test
    public void withCommonPrefixes() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        String content = "hello, s3!!!";
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, null);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, Map.of("key", "value"));
        putObjectRaw(s3, "bucket", "file.txt", content, null);

        String nextMarker = listObjects(s3, "bucket", 1000, "", "/", null, "folder1/",
                false, List.of( buildS3Object("file.txt", ROOT_USER, content)),
                List.of(CommonPrefix.builder().prefix("folder1/").build()));
        nextMarker = listObjects(s3, "bucket", 1000, nextMarker, "/", null,
                "folder2/", false,
                List.of(buildS3Object("folder1/file.txt", ROOT_USER, content)),
                List.of(CommonPrefix.builder().prefix("folder1/folder2/").build()));
        listObjects(s3, "bucket", 1000, nextMarker, "/", null,
                null, false,
                List.of(buildS3Object("folder1/folder2/file.txt", ROOT_USER, content)),
                List.of());
    }

    @Test
    public void simpleListObjects() {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        String content = "hello, s3!!!";
        putObjectRaw(s3, "bucket", "folder1/folder2/file.txt", content, null);
        putObjectRaw(s3, "bucket", "folder1/file.txt", content, Map.of("key", "value"));
        putObjectRaw(s3, "bucket", "file.txt", content, null);

        listObjects(s3, "bucket", 1000, "", null, null,
                null, false, List.of(
                        buildS3Object("file.txt", ROOT_USER, content),
                        buildS3Object("folder1/folder2/file.txt", ROOT_USER, content),
                        buildS3Object("folder1/file.txt", ROOT_USER, content)),
                List.of());

        listObjects(s3, "bucket", 1, "", null, null,
                null, true, List.of(buildS3Object("file.txt", ROOT_USER, content)),
                List.of());

    }

    public String listObjects(S3Client s3, String bucket, Integer maxKeys, String prefix, String delimeter,
                              String marker, String nextMarker, boolean isTruncated, List<S3Object> expectedContents,
                              List<CommonPrefix> expectedCommonPrefixes) {
        ListObjectsRequest.Builder request = ListObjectsRequest.builder()
                .bucket(bucket);
        if (maxKeys != null) {
            request.maxKeys(maxKeys);
        }
        request.prefix(prefix);
        if (delimeter != null) {
            request.delimiter(delimeter);
        }
        if (marker != null) {
            request.marker(marker);
        }
        ListObjectsResponse response = s3.listObjects(request.build());
        Assertions.assertEquals(isTruncated, response.isTruncated());
        Assertions.assertEquals(maxKeys != null ? maxKeys : 1000, response.maxKeys());
        Assertions.assertEquals(marker, response.marker());
        Assertions.assertEquals(prefix != null ? prefix : "", response.prefix());
        Assertions.assertEquals(bucket, response.name());
        Assertions.assertEquals(expectedContents.size(), response.contents().size());
        Assertions.assertEquals(expectedCommonPrefixes.size(), response.commonPrefixes().size());

        for (S3Object expectedS3Object : expectedContents) {
            Assertions.assertTrue(response.contents().stream()
                    .anyMatch(actualS3Object -> equalsS3Objects(expectedS3Object, actualS3Object)));
        }
        for (CommonPrefix commonPrefix : expectedCommonPrefixes) {
            Assertions.assertTrue(response.commonPrefixes().stream()
                    .anyMatch(actualCommonPrefix -> actualCommonPrefix.prefix().equals(commonPrefix.prefix())));
        }
        return response.nextMarker();
    }



}
