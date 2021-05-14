package com.thorinhood.multipart;

import com.thorinhood.BaseTest;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MultipartTest extends BaseTest {

    public MultipartTest() {
        super("testS3Java", 9999);
    }

    @Test
    public void simpleMultipart() throws IOException {
        S3Client s3Client = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3Client, "bucket");

        CreateMultipartUploadResponse response = s3Client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .key("folder/bigfile.txt")
                .bucket("bucket")
                .build());
        Assertions.assertEquals("bucket", response.bucket());
        Assertions.assertEquals("folder/bigfile.txt", response.key());

        String uploadId = response.uploadId();

        String part = createContent(5 * 1024 * 1024);
        List<CompletedPart> completedParts = uploadParts(s3Client, "bucket", "folder/bigfile.txt", uploadId,
                5, part);

        CompleteMultipartUploadResponse completeResponse = s3Client.completeMultipartUpload(
                CompleteMultipartUploadRequest.builder()
                    .uploadId(uploadId)
                    .bucket("bucket")
                    .key("folder/bigfile.txt")
                    .multipartUpload(CompletedMultipartUpload.builder()
                            .parts(completedParts)
                            .build())
                    .build());
        Assertions.assertEquals("bucket", completeResponse.bucket());
        Assertions.assertEquals("folder/bigfile.txt", completeResponse.key());
        String fullContent = IntStream.range(0, 5).mapToObj(i -> part).collect(Collectors.joining());
        String eTag = calcETag(fullContent);
        Assertions.assertEquals("\"" + eTag + "\"", completeResponse.eTag());
        checkObject("bucket", "folder", "bigfile.txt", fullContent,
                null, true);
    }

    @Test
    public void wrongEntityTooSmall() {
        S3Client s3Client = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3Client, "bucket");

        CreateMultipartUploadResponse response = s3Client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .key("folder/bigfile.txt")
                .bucket("bucket")
                .build());
        Assertions.assertEquals("bucket", response.bucket());
        Assertions.assertEquals("folder/bigfile.txt", response.key());

        String uploadId = response.uploadId();

        String part = createContent(3 * 1024 * 1024);

        UploadPartResponse uploadPartResponse = s3Client.uploadPart(UploadPartRequest.builder()
                .uploadId(uploadId)
                .partNumber(1)
                .bucket("bucket")
                .key("folder/bigfile.txt")
                .build(), RequestBody.fromString(part));
        assertException(HttpResponseStatus.BAD_REQUEST.code(), S3ResponseErrorCodes.ENTITY_TOO_SMALL, () -> {
            s3Client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                    .key("folder/bigfile.txt")
                    .bucket("bucket")
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder()
                            .parts(CompletedPart.builder()
                                    .partNumber(1)
                                    .eTag(uploadPartResponse.eTag())
                                    .build())
                            .build())
                    .build());
        });
    }

    @Test
    public void wrongPartUploadNumber() {
        S3Client s3Client = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3Client, "bucket");

        String part = createContent(5 * 1024 * 1024);

        CreateMultipartUploadResponse response = s3Client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .key("folder/bigfile.txt")
                .bucket("bucket")
                .build());
        String uploadId = response.uploadId();
        assertException(HttpResponseStatus.BAD_REQUEST.code(), S3ResponseErrorCodes.INVALID_ARGUMENT, () -> {
            s3Client.uploadPart(UploadPartRequest.builder()
                    .uploadId(uploadId)
                    .partNumber(0)
                    .bucket("bucket")
                    .key("folder/bigfile.txt")
                    .build(), RequestBody.fromString(part));
        });
        assertException(HttpResponseStatus.BAD_REQUEST.code(), S3ResponseErrorCodes.INVALID_ARGUMENT, () -> {
            s3Client.uploadPart(UploadPartRequest.builder()
                    .uploadId(uploadId)
                    .partNumber(10001)
                    .bucket("bucket")
                    .key("folder/bigfile.txt")
                    .build(), RequestBody.fromString(part));
        });
    }

    @Test
    public void invalidPart() {
        S3Client s3Client = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3Client, "bucket");

        String part = createContent(5 * 1024 * 1024);

        CreateMultipartUploadResponse response = s3Client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .key("folder/bigfile.txt")
                .bucket("bucket")
                .build());
        String uploadId = response.uploadId();
        List<CompletedPart> completedParts = uploadParts(s3Client, "bucket", "folder/bigfile.txt", uploadId,
                3, part);
        completedParts.add(CompletedPart.builder()
                .eTag(calcETag(part))
                .partNumber(10)
                .build());
        assertException(HttpResponseStatus.BAD_REQUEST.code(), S3ResponseErrorCodes.INVALID_PART, () -> {
            s3Client.completeMultipartUpload(
                    CompleteMultipartUploadRequest.builder()
                            .uploadId(uploadId)
                            .bucket("bucket")
                            .key("folder/bigfile.txt")
                            .multipartUpload(CompletedMultipartUpload.builder()
                                    .parts(completedParts)
                                    .build())
                            .build());
        });
    }

    @Test
    public void wrongNoSuchUpload() throws IOException {
        S3Client s3Client = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3Client, "bucket");

        String part = createContent(5 * 1024 * 1024);

        assertException(HttpResponseStatus.NOT_FOUND.code(), S3ResponseErrorCodes.NO_SUCH_UPLOAD, () -> {
            s3Client.uploadPart(UploadPartRequest.builder()
                    .uploadId("6WW")
                    .partNumber(1)
                    .bucket("bucket")
                    .key("folder/bigfile.txt")
                    .build(), RequestBody.fromString(part));
        });

        CreateMultipartUploadResponse response = s3Client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .key("folder/bigfile.txt")
                .bucket("bucket")
                .build());

        String uploadId = response.uploadId();
        List<CompletedPart> completedParts = uploadParts(s3Client, "bucket", "folder/bigfile.txt", uploadId,
                3, part);

        CompleteMultipartUploadResponse completeResponse = s3Client.completeMultipartUpload(
                CompleteMultipartUploadRequest.builder()
                        .uploadId(uploadId)
                        .bucket("bucket")
                        .key("folder/bigfile.txt")
                        .multipartUpload(CompletedMultipartUpload.builder()
                                .parts(completedParts)
                                .build())
                        .build());
        String fullContent = IntStream.range(0, 3).mapToObj(i -> part).collect(Collectors.joining());
        String eTag = calcETag(fullContent);
        Assertions.assertEquals("\"" + eTag + "\"", completeResponse.eTag());
        checkObject("bucket", "folder", "bigfile.txt", fullContent,
                null, true);

        assertException(HttpResponseStatus.NOT_FOUND.code(), S3ResponseErrorCodes.NO_SUCH_UPLOAD, () -> {
            s3Client.uploadPart(UploadPartRequest.builder()
                    .uploadId(uploadId)
                    .partNumber(6)
                    .bucket("bucket")
                    .key("folder/bigfile.txt")
                    .build(), RequestBody.fromString(part));
        });
    }

    @Test
    public void abortMultipartUpload() {
        S3Client s3Client = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3Client, "bucket");

        String part = createContent(5 * 1024 * 1024);

        CreateMultipartUploadResponse response = s3Client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .key("folder/bigfile.txt")
                .bucket("bucket")
                .build());

        String uploadId = response.uploadId();
        uploadParts(s3Client, "bucket", "folder/bigfile.txt", uploadId, 5, part);

        s3Client.abortMultipartUpload(
                AbortMultipartUploadRequest.builder()
                    .key("folder/bigfile.txt")
                    .bucket("bucket")
                    .uploadId(uploadId)
                    .build());
        Assertions.assertFalse(checkFolder("bucket/folder"));

        response = s3Client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .key("bigfile.txt")
                .bucket("bucket")
                .build());
        uploadId = response.uploadId();
        uploadParts(s3Client, "bucket", "bigfile.txt", uploadId, 5, part);
        s3Client.abortMultipartUpload(
                AbortMultipartUploadRequest.builder()
                        .key("bigfile.txt")
                        .bucket("bucket")
                        .uploadId(uploadId)
                        .build());
        Assertions.assertFalse(checkFolder("bucket/.#bigfile.txt"));
    }


    private List<CompletedPart> uploadParts(S3Client s3Client, String bucket, String key, String uploadId, int count,
                                            String content) {
        List<CompletedPart> parts = new ArrayList<>();
        String eTag = calcETag(content);
        for (int i = 1; i <= count; i++) {
            UploadPartResponse response = s3Client.uploadPart(UploadPartRequest.builder()
                    .uploadId(uploadId)
                    .partNumber(i)
                    .bucket(bucket)
                    .key(key)
                    .build(), RequestBody.fromString(content));
            Assertions.assertEquals("\"" + eTag + "\"", response.eTag());
            parts.add(CompletedPart.builder()
                    .eTag(response.eTag())
                    .partNumber(i)
                    .build());
        }
        return parts;
    }
}
