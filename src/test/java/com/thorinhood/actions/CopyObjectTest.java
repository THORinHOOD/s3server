package com.thorinhood.actions;

import com.thorinhood.BaseTest;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class CopyObjectTest extends BaseTest {

    public CopyObjectTest() {
        super("testS3Java", 9999);
    }

    @Test
    public void simpleCopy() throws IOException {
        String bucket = "bucket";
        String sourceKeyWithoutBucket = "folder/file.txt";
        String content = createContent(5 * 1024 * 1024);
        String targetKey = "folder2/file.txt";
        Map<String, String> metadata = Map.of(
                "key1", "value1",
                "key2", "value2"
        );
        S3Client s3Client = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3Client, bucket);
        putObjectRaw(s3Client, bucket, sourceKeyWithoutBucket, content, metadata);
        CopyObjectResponse response = s3Client.copyObject(CopyObjectRequest.builder()
                .copySource("/" + bucket + File.separatorChar + sourceKeyWithoutBucket)
                .destinationKey(targetKey)
                .destinationBucket(bucket)
                .build());
        checkObject(bucket, "folder",  "file.txt", content, metadata);
        checkObject(bucket, "folder2", "file.txt", content, metadata);
        Assertions.assertEquals("\"" + calcETag(content) + "\"", response.copyObjectResult().eTag());

        String bucket2 = "bucket2";
        createBucketRaw(s3Client, bucket2);
        response = s3Client.copyObject(CopyObjectRequest.builder()
                .copySource("/" + bucket + File.separatorChar + sourceKeyWithoutBucket)
                .destinationBucket(bucket2)
                .destinationKey(sourceKeyWithoutBucket)
                .build());
        checkObject(bucket, "folder",  "file.txt", content, metadata);
        checkObject(bucket2, "folder", "file.txt", content, metadata);
        Assertions.assertEquals("\"" + calcETag(content) + "\"", response.copyObjectResult().eTag());
    }

    @Test
    public void copyObjectMatches() throws IOException {
        String bucket = "bucket";
        String sourceKeyWithoutBucket = "folder/file.txt";
        String content = createContent(5 * 1024 * 1024);
        String targetKey = "folder2/file.txt";
        S3Client s3Client = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3Client, bucket);
        putObjectRaw(s3Client, bucket, sourceKeyWithoutBucket, content, null);
        CopyObjectResponse response = s3Client.copyObject(CopyObjectRequest.builder()
                .copySource("/" + bucket + File.separatorChar + sourceKeyWithoutBucket)
                .destinationKey(targetKey)
                .destinationBucket(bucket)
                .copySourceIfMatch("\"" + calcETag(content) + "\"")
                .copySourceIfNoneMatch("w0Y2qfo")
                .build());
        checkObject(bucket, "folder",  "file.txt", content, null);
        checkObject(bucket, "folder2", "file.txt", content, null);
        Assertions.assertEquals("\"" + calcETag(content) + "\"", response.copyObjectResult().eTag());

        try {
            s3Client.copyObject(CopyObjectRequest.builder()
                .copySource("/" + bucket + File.separatorChar + sourceKeyWithoutBucket)
                .destinationKey("key.txt")
                .destinationBucket(bucket)
                .copySourceIfMatch("14dC7U0q")
                .build());
            Assertions.fail("Must be exception");
        } catch (S3Exception exception) {
            Assertions.assertEquals(HttpResponseStatus.PRECONDITION_FAILED.code(), exception.statusCode());
        }

        try {
            s3Client.copyObject(CopyObjectRequest.builder()
                .copySource("/" + bucket + File.separatorChar + sourceKeyWithoutBucket)
                .destinationKey("key.txt")
                .destinationBucket(bucket)
                .copySourceIfNoneMatch("\"" + calcETag(content) + "\"")
                .build());
            Assertions.fail("Must be exception");
        } catch (S3Exception exception) {
            Assertions.assertEquals(HttpResponseStatus.PRECONDITION_FAILED.code(), exception.statusCode());
        }
    }

    @Test
    public void copyObjectPermission() throws IOException {
        String bucket = "bucket";
        String bucket2 = "bucket2";
        String sourceKeyWithoutBucket = "folder/file.txt";
        String content = createContent(5 * 1024 * 1024);
        S3Client s3Client = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3Client, bucket);
        putObjectRaw(s3Client, bucket, sourceKeyWithoutBucket, content, null);
        S3Client s3Client2 = getS3Client(true, ROOT_USER_2.getAccessKey(), ROOT_USER_2.getSecretKey());
        createBucketRaw(s3Client2, bucket2);
        assertException(HttpResponseStatus.FORBIDDEN.code(), S3ResponseErrorCodes.ACCESS_DENIED, () -> {
            s3Client.copyObject(CopyObjectRequest.builder()
                    .copySource("/" + bucket + File.separatorChar + sourceKeyWithoutBucket)
                    .destinationKey("key.txt")
                    .destinationBucket(bucket2)
                    .build());
        });
        assertException(HttpResponseStatus.FORBIDDEN.code(), S3ResponseErrorCodes.ACCESS_DENIED, () -> {
            s3Client2.copyObject(CopyObjectRequest.builder()
                    .copySource("/" + bucket + File.separatorChar + sourceKeyWithoutBucket)
                    .destinationKey("key.txt")
                    .destinationBucket(bucket2)
                    .build());
        });
        s3Client.putBucketPolicy(PutBucketPolicyRequest.builder()
                .bucket(bucket)
                .policy("{\n" +
                        "    \"Statement\" : [\n" +
                        "        {\n" +
                        "            \"Effect\" : \"Allow\",\n" +
                        "            \"Action\" : [\"*\"],\n" +
                        "            \"Resource\" : \"arn:aws:s3:::bucket/*\",\n" +
                        "            \"Principal\" : {\n" +
                        "                \"AWS\": \"" + ROOT_USER_2.getArn() + "\"\n"  +
                        "            }\n" +
                        "        }]\n" +
                        "}")
                .build());
        CopyObjectResponse response = s3Client2.copyObject(CopyObjectRequest.builder()
                .copySource("/" + bucket + File.separatorChar + sourceKeyWithoutBucket)
                .destinationKey("key.txt")
                .destinationBucket(bucket2)
                .build());
        checkObject(bucket, "folder", "file.txt",  content, null);
        checkObject(bucket2, null, "key.txt",  content, null);
        Assertions.assertEquals("\"" + calcETag(content) + "\"", response.copyObjectResult().eTag());

        s3Client2.putBucketPolicy(PutBucketPolicyRequest.builder()
                .bucket(bucket2)
                .policy("{\n" +
                        "    \"Statement\" : [\n" +
                        "        {\n" +
                        "            \"Effect\" : \"Allow\",\n" +
                        "            \"Action\" : [\"*\"],\n" +
                        "            \"Resource\" : \"arn:aws:s3:::bucket2/*\",\n" +
                        "            \"Principal\" : {\n" +
                        "                \"AWS\": \"" + ROOT_USER.getArn() + "\"\n"  +
                        "            }\n" +
                        "        }]\n" +
                        "}")
                .build());
        response = s3Client.copyObject(CopyObjectRequest.builder()
                .copySource("/" + bucket + File.separatorChar + sourceKeyWithoutBucket)
                .destinationKey("folder/key.txt")
                .destinationBucket(bucket2)
                .build());
        checkObject(bucket, "folder", "file.txt",  content, null);
        checkObject(bucket2, "folder", "key.txt",  content, null);
        Assertions.assertEquals("\"" + calcETag(content) + "\"", response.copyObjectResult().eTag());
    }

}
