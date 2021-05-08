package com.thorinhood.utils.utils;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.*;

import java.net.URI;
import java.time.Duration;

public class SdkUtil {

    public static S3AsyncClient buildAsync(int port, Region region, boolean chunked, String accessKey,
                                           String secretKey) {
        S3AsyncClientBuilder s3 = S3AsyncClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey,
                        secretKey)))
                .httpClient(NettyNioAsyncHttpClient.builder().readTimeout(Duration.ofSeconds(240)).build())
                .endpointOverride(URI.create("http://localhost:" + port))
                .region(region);
        if (!chunked) {
            s3.serviceConfiguration(S3Configuration.builder()
                    .chunkedEncodingEnabled(false)
                    .build());
        }
        return s3.build();
    }

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
