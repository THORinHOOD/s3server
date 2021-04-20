package com.thorinhood.utils.actions;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class PutObjectTest extends BaseTest {

    public PutObjectTest() {
        super("/home/thorinhood/testS3Java", 9999);
    }

    @Test
    public void putObjectSimple() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucket("bucket", s3);
        putObject(s3,
                "bucket",
                null,
                "file.txt",
                "hello, s3!!!");
    }

    @Test
    public void putObjectCompositeKey() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucket("bucket", s3);
        putObject(s3,
                "bucket",
                "folder1/folder2",
                "file.txt",
                "hello, s3!!!");
    }

    @Test
    public void putTwoObjectsCompositeKey() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucket("bucket", s3);
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
        checkPutObject("bucket", "folder1", "file.txt", "hello, s3!!!");
    }

    @Test
    public void putTwoObjectsInDifferentBuckets() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucket("bucket", s3);
        createBucket("bucket2", s3);
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
        checkPutObject("bucket", "folder1", "file.txt", "hello, s3!!!");
        checkPutObject("bucket2", "folder1", "file.txt", "hello, s3!!!");
    }

    @Test
    public void putChunkedFile() throws Exception {
        S3Client s3 = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucket("bucket", s3);
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
        checkPutObject("bucket", "folder1", "file.txt", "hello, s3!!!");
        checkPutObject("bucket", "folder1", "file2.txt", "hello, s3, again!!!");
    }

    @Test
    public void putObjectInWrongBucket() throws Exception {
        S3Client s3 = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucket("bucket", s3);
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
        createBucket("bucket", s3);
        putObject(s3,
                "bucket",
                null,
                "файл.txt",
                "привет, s3!!!");
    }

    private void putObject(S3Client s3Client, String bucket, String keyWithoutName, String fileName, String content)
            throws IOException {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(buildKey(keyWithoutName, fileName))
                .build();
        PutObjectResponse response = s3Client.putObject(request, RequestBody.fromString(content));
        Assertions.assertEquals(response.eTag(), DigestUtils.md5Hex(content.getBytes()));
        checkPutObject(bucket, keyWithoutName, fileName, content);
    }

    private void checkPutObject(String bucket, String keyWithoutName, String fileName, String content) throws IOException {
        File file = new File(BASE_PATH + File.separatorChar + bucket + File.separatorChar +
                buildKey(keyWithoutName, fileName));
        Assertions.assertTrue(file.exists() && file.isFile());
        File acl = new File(BASE_PATH + File.separatorChar + bucket + File.separatorChar +
                (keyWithoutName == null || keyWithoutName.isEmpty() ? ".#" + fileName + File.separatorChar +
                        fileName + ".acl" :
                        keyWithoutName + File.separatorChar + ".#" + fileName + File.separatorChar + fileName + ".acl"));
        Assertions.assertTrue(acl.exists() && acl.isFile());
        checkContent(file, content);
    }

    private void checkContent(File file, String expected) throws IOException {
        String actual = Files.readString(file.toPath());
        Assertions.assertEquals(expected, actual);
    }

    private void createBucket(String bucket, S3Client s3Client) {
        CreateBucketRequest request = CreateBucketRequest.builder()
                .bucket(bucket)
                .build();
        try {
            s3Client.createBucket(request);
        } catch (Exception exception) {
        }
    }

    private String buildKey(String keyWithoutFileName, String fileName) {
        return (keyWithoutFileName == null || keyWithoutFileName.isEmpty() ? fileName : keyWithoutFileName +
                File.separatorChar + fileName);
    }

}
