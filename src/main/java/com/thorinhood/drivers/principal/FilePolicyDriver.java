package com.thorinhood.drivers.principal;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thorinhood.data.S3FileBucketPath;
import com.thorinhood.data.policy.BucketPolicy;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import com.thorinhood.drivers.FileDriver;
import com.thorinhood.exceptions.S3Exception;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;

public class FilePolicyDriver extends FileDriver implements PolicyDriver {

    private final ObjectMapper objectMapper;

    public FilePolicyDriver(String baseFolderPath, String configFolderPath, String usersFolderPath) {
        super(baseFolderPath, configFolderPath, usersFolderPath);
        this.objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    @Override
    public void putBucketPolicy(S3FileBucketPath s3FileBucketPath, byte[] bytes) throws S3Exception {
        try {
            objectMapper.readValue(bytes, BucketPolicy.class);
        } catch (IOException exception) {
            throw S3Exception.builder("Can't parse bucket policy")
                    .setStatus(HttpResponseStatus.BAD_REQUEST)
                    .setCode(S3ResponseErrorCodes.INVALID_REQUEST)
                    .setMessage(exception.getMessage())
                    .build();
        }
        Path target = new File(s3FileBucketPath.getPathToBucketPolicyFile()).toPath();
        Path source = createPreparedTmpFile(new File(s3FileBucketPath.getPathToBucketMetadataFolder()).toPath(), target,
                bytes);
        commitFile(source, target);
    }

    @Override
    public Optional<BucketPolicy> getBucketPolicy(S3FileBucketPath s3FileBucketPath) throws S3Exception {
        File file = new File(s3FileBucketPath.getPathToBucketPolicyFile());
        if (!file.exists() || !file.isFile()) {
            return Optional.empty();
        }
        try {
            return Optional.of(objectMapper.readValue(file, BucketPolicy.class));
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception);
        }
    }

    @Override
    public byte[] convertBucketPolicy(S3FileBucketPath s3FileBucketPath, BucketPolicy bucketPolicy) throws S3Exception {
        try {
            return objectMapper.writeValueAsString(bucketPolicy).getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            throw S3Exception.INTERNAL_ERROR("Can't parse bucket policy to json");
        }
    }

}
