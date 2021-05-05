package com.thorinhood.drivers.principal;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thorinhood.data.S3BucketPath;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import com.thorinhood.data.policy.BucketPolicy;
import com.thorinhood.drivers.FileDriver;
import com.thorinhood.exceptions.S3Exception;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class FilePolicyDriver extends FileDriver implements PolicyDriver {

    private static final String POSTFIX_POLICY_FILE = "-policy.json";

    private final ObjectMapper objectMapper;

    public FilePolicyDriver(String baseFolderPath, String configFolderPath, String usersFolderPath) {
        super(baseFolderPath, configFolderPath, usersFolderPath);
        this.objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    @Override
    public void putBucketPolicy(S3BucketPath s3BucketPath, byte[] bytes) throws S3Exception {
        BucketPolicy bucketPolicy = null;
        try {
            bucketPolicy = objectMapper.readValue(bytes, BucketPolicy.class);
        } catch (IOException exception) {
            throw S3Exception.build("Can't parse bucket policy")
                    .setStatus(HttpResponseStatus.BAD_REQUEST)
                    .setCode(S3ResponseErrorCodes.INVALID_REQUEST)
                    .setMessage(exception.getMessage()) // TODO
                    .setResource("1")
                    .setRequestId("1");
        }
        putBucketPolicy(s3BucketPath, bucketPolicy);
    }

    @Override
    public void putBucketPolicy(S3BucketPath s3BucketPath, BucketPolicy bucketPolicy) throws S3Exception {
        String pathToBucketPolicyFile = getPathToBucketPolicyFile(s3BucketPath, true);
        try {
            objectMapper.writeValue(new File(pathToBucketPolicyFile), bucketPolicy);
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't write bucket policy file")
                    .setMessage("Can't write bucket policy file")
                    .setResource("1")
                    .setRequestId("1");
        }
    }

    @Override
    public Optional<BucketPolicy> getBucketPolicy(S3BucketPath s3BucketPath) throws S3Exception {
        String pathToBucketPolicy = getPathToBucketPolicyFile(s3BucketPath, false);
        BucketPolicy bucketPolicy = null;
        File file = new File(pathToBucketPolicy);
        if (!file.exists() || !file.isFile()) {
            return Optional.empty();
        }
        try {
            bucketPolicy = objectMapper.readValue(file, BucketPolicy.class);
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't find bucket policy")
                    .setMessage("Can't find bucket policy")
                    .setResource("1")
                    .setRequestId("1");
        }
        return Optional.of(bucketPolicy);
    }

    @Override
    public byte[] convertBucketPolicy(BucketPolicy bucketPolicy) throws S3Exception {
        try {
            return objectMapper.writeValueAsString(bucketPolicy).getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            throw S3Exception.INTERNAL_ERROR("Can't parse bucket policy to json")
                    .setMessage("Can't parse bucket policy to json")
                    .setResource("1")
                    .setRequestId("1");
        }
    }

    private String getPathToBucketPolicyFile(S3BucketPath s3BucketPath, boolean safely) {
        return getPathToBucketMetadataFolder(s3BucketPath, safely) + File.separatorChar + s3BucketPath.getBucket() +
                POSTFIX_POLICY_FILE;
    }

}
