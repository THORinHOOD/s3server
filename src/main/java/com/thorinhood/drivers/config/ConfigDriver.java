package com.thorinhood.drivers.config;

import com.thorinhood.data.S3User;

import java.util.Optional;

public interface ConfigDriver {

    void init() throws Exception;
    void addUser(S3User s3User) throws Exception;
    Optional<S3User> getS3User(String accessKey) throws Exception;
    void removeUser(String accessKey) throws Exception;

}
