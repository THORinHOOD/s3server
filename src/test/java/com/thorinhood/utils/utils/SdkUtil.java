package com.thorinhood.utils.utils;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;

public class SdkUtil {

    public static S3Client build(int port, Region region, boolean chunked, String accessKey, String secretKey) {
        S3ClientBuilder s3 = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey,
                        secretKey)))
                .endpointOverride(URI.create("http://localhost:" + port))
                                .region(region);
        if (!chunked) {
            s3.serviceConfiguration(S3Configuration.builder()
                    .chunkedEncodingEnabled(false)
                    .build());
        }
        return s3.build();
    }

}
