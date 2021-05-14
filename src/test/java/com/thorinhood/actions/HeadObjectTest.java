package com.thorinhood.actions;

import com.thorinhood.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.util.Map;

public class HeadObjectTest extends BaseTest {
    public HeadObjectTest() {
        super("testS3Java", 9999);
    }

    @Test
    public void simpleHeadObject() {
        S3Client s3Client = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3Client, "bucket");
        String content = createContent(5 * 1024 * 1024);
        String eTag = calcETag(content);
        Map<String, String> metadata = Map.of(
                "key1", "value1",
                "key2", "value2"
        );
        putObjectRaw(s3Client, "bucket", "file.txt", content, metadata);
        HeadObjectResponse response = s3Client.headObject(HeadObjectRequest.builder()
                .key("file.txt")
                .bucket("bucket")
                .ifMatch(eTag)
                .build());
        Assertions.assertEquals("\"" + eTag + "\"", response.eTag());
        Assertions.assertTrue(equalsMaps(metadata, response.metadata()));
        Assertions.assertEquals(content.getBytes().length, response.contentLength().intValue());
        Assertions.assertEquals("text/plain", response.contentType());
    }

}
