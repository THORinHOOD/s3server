package com.thorinhood.drivers.main;

import com.thorinhood.data.S3FileObjectPath;
import com.thorinhood.data.S3User;
import com.thorinhood.exceptions.S3Exception;

public interface AclPermissionChecker {
    boolean checkAclPermission(boolean isBucketAcl, S3FileObjectPath s3FileObjectPath, String methodName,
                               S3User s3User) throws S3Exception;
    boolean isOwner(boolean isBucket, S3FileObjectPath s3FileObjectPath, S3User s3User) throws S3Exception;
}
