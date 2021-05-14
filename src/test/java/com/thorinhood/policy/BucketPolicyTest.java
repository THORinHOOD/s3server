package com.thorinhood.policy;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thorinhood.BaseTest;
import com.thorinhood.data.policy.BucketPolicy;
import com.thorinhood.data.policy.Statement;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyResponse;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class BucketPolicyTest extends BaseTest {
    public BucketPolicyTest() {
        super("testS3Java", 9999);
    }

    @Test
    public void putAllowPolicy() throws IOException {
        S3Client s3Client = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        S3Client s3Client2 = getS3Client(false, ROOT_USER_2.getAccessKey(), ROOT_USER_2.getSecretKey());

        createBucketRaw(s3Client, "bucket");
        String content = createContent(500);
        Map<String, String> metadata = Map.of("key1", "value1");
        assertException(HttpResponseStatus.FORBIDDEN.code(), S3ResponseErrorCodes.ACCESS_DENIED, () -> {
            putObjectRaw(s3Client2, "bucket", "file.txt", content, metadata);
        });
        s3Client.putBucketPolicy(PutBucketPolicyRequest.builder()
            .bucket("bucket")
            .policy("{\n" +
                    "    \"Statement\" : [\n" +
                    "        {\n" +
                    "            \"Effect\" : \"Allow\",\n" +
                    "            \"Action\" : [\"s3:PutObject\", \"s3:GetObject\"],\n" +
                    "            \"Resource\" : \"arn:aws:s3:::bucket/file.txt\",\n" +
                    "            \"Principal\" : {\n" +
                    "                \"AWS\": \"" + ROOT_USER_2.getArn() + "\"\n" +
                    "            }\n" +
                    "        }]\n" +
                    "}")
            .build());

        putObjectRaw(s3Client2, "bucket", "file.txt", content, metadata);
        checkObject("bucket", null, "file.txt", content, metadata);
    }

    @Test
    public void putDenyPolicy() {
        S3Client s3Client = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3Client, "bucket");
        String content = createContent(500);
        Map<String, String> metadata = Map.of("key1", "value1");
        s3Client.putBucketPolicy(PutBucketPolicyRequest.builder()
                .bucket("bucket")
                .policy("{\n" +
                        "    \"Statement\" : [\n" +
                        "        {\n" +
                        "            \"Effect\" : \"Deny\",\n" +
                        "            \"Action\" : [\"s3:PutObject\", \"s3:GetObject\"],\n" +
                        "            \"Resource\" : \"arn:aws:s3:::bucket/*\",\n" +
                        "            \"Principal\" : {\n" +
                        "                \"AWS\": \"" + ROOT_USER.getArn() + "\"\n" +
                        "            }\n" +
                        "        }]\n" +
                        "}")
                .build());

        assertException(HttpResponseStatus.FORBIDDEN.code(), S3ResponseErrorCodes.ACCESS_DENIED, () -> {
            putObjectRaw(s3Client, "bucket", "file.txt", content, metadata);
        });
    }

    @Test
    public void getBucketPolicy() throws IOException {
        S3Client s3Client = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3Client, "bucket");
        String policy = "{" +
                "    \"Statement\" : [" +
                "        {" +
                "            \"Effect\" : \"Allow\"," +
                "            \"Action\" : [\"s3:PutObject\", \"s3:GetObject\"]," +
                "            \"Resource\" : \"arn:aws:s3:::bucket/*\"," +
                "            \"Principal\" : {\n" +
                "                \"AWS\": \"" + ROOT_USER_2.getArn() + "\"" +
                "            }" +
                "        }]" +
                "}";
        s3Client.putBucketPolicy(PutBucketPolicyRequest.builder()
                .bucket("bucket")
                .policy(policy)
                .build());

        GetBucketPolicyResponse response = s3Client.getBucketPolicy(GetBucketPolicyRequest.builder()
                .bucket("bucket")
                .build());
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        BucketPolicy bucketPolicyActual = objectMapper.readValue(response.policy().getBytes(StandardCharsets.UTF_8),
                BucketPolicy.class);
        BucketPolicy bucketPolicyExpected = objectMapper.readValue(policy, BucketPolicy.class);
        Assertions.assertEquals(bucketPolicyExpected.getId(), bucketPolicyActual.getId());
        Assertions.assertEquals(bucketPolicyExpected.getVersion(), bucketPolicyActual.getVersion());
        Assertions.assertEquals(1, bucketPolicyActual.getStatements().size());
        Statement statementExpected = bucketPolicyExpected.getStatements().get(0);
        Statement statementActual = bucketPolicyActual.getStatements().get(0);
        Assertions.assertEquals(statementExpected.getEffect(), statementActual.getEffect());
        Assertions.assertEquals(statementExpected.getSid(), statementActual.getSid());
        Assertions.assertEquals(new HashSet<>(statementExpected.getAction()),
                                new HashSet<>(statementActual.getAction()));
        Assertions.assertEquals(new HashSet<>(statementExpected.getPrinciple().getAWS()),
                new HashSet<>(statementActual.getPrinciple().getAWS()));
    }

    @Test
    public void getBucketPolicyEmpty() {
        S3Client s3Client = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        createBucketRaw(s3Client, "bucket");

        assertException(HttpResponseStatus.NOT_FOUND.code(), S3ResponseErrorCodes.INVALID_REQUEST, () ->
                s3Client.getBucketPolicy(GetBucketPolicyRequest.builder()
                        .bucket("bucket")
                        .build()));
    }
}
