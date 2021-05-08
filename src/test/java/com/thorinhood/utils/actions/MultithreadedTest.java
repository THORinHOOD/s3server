package com.thorinhood.utils.actions;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class MultithreadedTest extends BaseTest {

    public MultithreadedTest() {
        super("testS3Java", 9999);
    }

    @Test
    public void putGetWhileOverwrite() throws Exception {
        S3Client s3 = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        S3AsyncClient s3Async = getS3AsyncClient(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3, "bucket");
        String firstContent = createContent(100);
        String secondContent = createContent(200);
        Map<String, String> metadataFirst = Map.of("key1", "value1");
        Map<String, String> metadataSecond = Map.of("key2", "value2");
        List<String> contents = new ArrayList<>();
        List<Map<String, String>> metadatas = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            contents.add(i % 2 == 0 ? firstContent : secondContent);
            metadatas.add(i % 2 == 0 ? metadataFirst : metadataSecond);
        }
        putObjectRaw(s3, "bucket", "folder1/file.txt", firstContent, metadataFirst);
        List<CompletableFuture<PutObjectResponse>> putFutureList = putObjectAsync(s3Async, "bucket",
                "folder1", "file.txt", contents, metadatas);
        List<CompletableFuture<ResponseBytes<GetObjectResponse>>> getFutureList = getObjectAsync(s3Async,
                "bucket", "folder1/file.txt", null, null, 50);
        checkGetObjectAsync(getFutureList, List.of(firstContent, secondContent),
                List.of(metadataFirst, metadataSecond));
        checkPutObjectAsync("bucket", "folder1", "file.txt", putFutureList,
                List.of(firstContent, secondContent), List.of(metadataFirst, metadataSecond));
        s3Async.close();
        s3.close();
    }

}
