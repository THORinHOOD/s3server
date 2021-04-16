package com.thorinhood.drivers.user;

import com.thorinhood.data.S3User;

import java.util.Optional;

public interface UserDriver {

    void addUser(S3User s3User) throws Exception;
    Optional<S3User> getS3User(String accessKey) throws Exception;
    void removeUser(String accessKey) throws Exception;

}
