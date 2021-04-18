package com.thorinhood.drivers.main;

import com.thorinhood.data.S3User;
import com.thorinhood.exceptions.S3Exception;

public interface AclPermissionChecker {
    boolean checkAclPermission(boolean isBucketAcl, String bucket, String key, String methodName,
                               S3User s3User) throws S3Exception;
    boolean isOwner(boolean isBucket, String bucket, String key, S3User s3User) throws S3Exception;
}
