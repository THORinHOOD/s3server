package com.thorinhood.data;

import java.beans.ConstructorProperties;

public class S3User {

    private final String accessKey;
    private final String secretKey;
    private final String path;
    private final String userName;
    private final String userId;
    private final String arn;
    private final String canonicalUserId;
    private final String accountName;

    @ConstructorProperties({"accessKey", "secretKey", "path", "userName", "userId", "arn", "canonicalUserId",
            "accountName"})
    public S3User(String accessKey, String secretKey, String path, String userName, String userId, String arn,
                  String canonicalUserId, String accountName) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.path = path;
        this.userName = userName;
        this.userId = userId;
        this.arn = arn;
        this.canonicalUserId = canonicalUserId;
        this.accountName = accountName;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getPath() {
        return path;
    }

    public String getUserName() {
        return userName;
    }

    public String getUserId() {
        return userId;
    }

    public String getArn() {
        return arn;
    }

    public String getCanonicalUserId() {
        return canonicalUserId;
    }

    public String getAccountName() {
        return accountName;
    }
}
