package com.thorinhood.utils.actions;

import com.thorinhood.data.S3ResponseErrorCodes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class PutObjectTest extends BaseTest {

    public PutObjectTest() {
        super("/home/thorinhood/testS3Java", 9999);
    }

    @Test
    public void putObjectSimple() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw("bucket", s3);
        putObject(s3,
                "bucket",
                null,
                "file.txt",
                "hello, s3!!!");
    }

    @Test
    public void putObjectCompositeKey() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw("bucket", s3);
        putObject(s3,
                "bucket",
                "folder1/folder2",
                "file.txt",
                "hello, s3!!!");
    }

    @Test
    public void putTwoObjectsCompositeKey() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw("bucket", s3);
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
        checkPutObject("bucket", "folder1", "file.txt", "hello, s3!!!",
                null);
    }

    @Test
    public void putTwoObjectsInDifferentBuckets() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw("bucket", s3);
        createBucketRaw("bucket2", s3);
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
        checkPutObject("bucket", "folder1", "file.txt", "hello, s3!!!",
                null);
        checkPutObject("bucket2", "folder1", "file.txt", "hello, s3!!!",
                null);
    }

    @Test
    public void putChunkedFile() throws Exception {
        S3Client s3 = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw("bucket", s3);
        putObject(s3,
                "bucket",
                "folder1",
                "file.txt",
                "hello, s3!!!");
        putObject(s3,
                "bucket",
                "folder1",
                "file2.txt",
                "hello, s3, again!!!");
        checkPutObject("bucket", "folder1", "file.txt", "hello, s3!!!",
                null);
        checkPutObject("bucket", "folder1", "file2.txt", "hello, s3, again!!!",
                null);
    }

    @Test
    public void putObjectInWrongBucket() throws Exception {
        S3Client s3 = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw("bucket", s3);
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
        createBucketRaw("bucket", s3);
        putObject(s3,
                "bucket",
                null,
                "файл.txt",
                "привет, s3!!!");
    }

    @Test
    public void putObjectWithMetaData() throws Exception {
        S3Client s3 = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw("bucket", s3);
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
        createBucketRaw("bucket", s3);
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
        createBucketRaw("bucket", s3);
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
        createBucketRaw("bucket", s3);
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
        Assertions.assertEquals(response.eTag(), calcETag(content));
        checkPutObject(bucket, keyWithoutName, fileName, content, metadata);
    }

    private void checkPutObject(String bucket, String keyWithoutName, String fileName, String content,
                                Map<String, String> metadata) throws IOException {
        File file = new File(BASE_PATH + File.separatorChar + bucket + File.separatorChar +
                buildKey(keyWithoutName, fileName));
        Assertions.assertTrue(file.exists() && file.isFile());
        File acl = new File(BASE_PATH + File.separatorChar + bucket + File.separatorChar +
                (keyWithoutName == null || keyWithoutName.isEmpty() ? ".#" + fileName + File.separatorChar +
                        fileName + ".acl" :
                        keyWithoutName + File.separatorChar + ".#" + fileName + File.separatorChar + fileName + ".acl"));
        Assertions.assertTrue(acl.exists() && acl.isFile());
        checkContent(file, content);
        if (metadata != null) {
            checkMetadata(bucket, keyWithoutName, fileName, metadata);
        }
    }

    private void checkMetadata(String bucket, String keyWithoutName, String fileName, Map<String, String> metadata)
            throws IOException {
        File metadataFile = new File(BASE_PATH + File.separatorChar + bucket + File.separatorChar +
                (keyWithoutName == null || keyWithoutName.isEmpty() ? ".#" + fileName + File.separatorChar +
                    fileName + ".meta" :
                    keyWithoutName + File.separatorChar + ".#" + fileName + File.separatorChar + fileName + ".meta"));
        Assertions.assertTrue(metadataFile.exists() && metadataFile.isFile());
        String metadataActualString = Files.readString(metadataFile.toPath());
        Map<String, String> actualMetadata = Arrays.stream(metadataActualString.split("\n"))
                .collect(Collectors.toMap(
                        line -> line.substring(0, line.indexOf("=")),
                        line -> line.substring(line.indexOf("=") + 1)
                ));
        assertMaps(metadata, actualMetadata);
    }

    private void checkContent(File file, String expected) throws IOException {
        String actual = Files.readString(file.toPath());
        Assertions.assertEquals(expected, actual);
    }

    private String buildKey(String keyWithoutFileName, String fileName) {
        return (keyWithoutFileName == null || keyWithoutFileName.isEmpty() ? fileName : keyWithoutFileName +
                File.separatorChar + fileName);
    }

}
