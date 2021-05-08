package com.thorinhood.drivers.acl;

import com.thorinhood.data.S3BucketPath;
import com.thorinhood.data.S3ObjectPath;
import com.thorinhood.data.acl.AccessControlPolicy;
import com.thorinhood.drivers.lock.PreparedOperationFileWrite;
import com.thorinhood.drivers.lock.PreparedOperationFileWriteWithResult;
import com.thorinhood.exceptions.S3Exception;

public interface AclDriver {

    PreparedOperationFileWriteWithResult<String> putObjectAcl(S3ObjectPath s3ObjectPath, AccessControlPolicy acl)
            throws S3Exception;
    AccessControlPolicy getObjectAcl(S3ObjectPath s3ObjectPath) throws S3Exception;
    PreparedOperationFileWrite putBucketAcl(S3BucketPath s3BucketPath, AccessControlPolicy acl) throws S3Exception;
    AccessControlPolicy getBucketAcl(S3BucketPath s3BucketPath) throws S3Exception;
    AccessControlPolicy parseFromBytes(byte[] bytes) throws S3Exception;

}
