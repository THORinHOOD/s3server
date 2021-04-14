package com.thorinhood.data;

public class S3User {

    private final String accessKey;
    private final String secretKey;

    public S3User(String accessKey, String secretKey) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }
}
