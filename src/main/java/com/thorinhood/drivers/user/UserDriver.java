package com.thorinhood.drivers.user;

import com.thorinhood.data.S3User;
import com.thorinhood.exceptions.S3Exception;

import java.util.Optional;

public interface UserDriver {

    void addUser(S3User s3User) throws S3Exception;
    void addUser(String pathToIdentity) throws Exception;
    Optional<S3User> getS3User(String accessKey) throws S3Exception;
    void removeUser(String accessKey) throws S3Exception;

}
