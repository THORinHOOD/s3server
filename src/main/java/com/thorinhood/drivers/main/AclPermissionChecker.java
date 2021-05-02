package com.thorinhood.drivers.main;

import com.thorinhood.data.S3ObjectPath;
import com.thorinhood.data.S3User;
import com.thorinhood.exceptions.S3Exception;

public interface AclPermissionChecker {
    boolean checkAclPermission(boolean isBucketAcl, S3ObjectPath s3ObjectPath, String methodName,
                               S3User s3User) throws S3Exception;
    boolean isOwner(boolean isBucket, S3ObjectPath s3ObjectPath, S3User s3User) throws S3Exception;
}
