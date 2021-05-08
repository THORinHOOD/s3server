package com.thorinhood.drivers.acl;

import com.thorinhood.data.S3FileBucketPath;
import com.thorinhood.data.S3FileObjectPath;
import com.thorinhood.data.acl.AccessControlPolicy;
import com.thorinhood.exceptions.S3Exception;

public interface AclDriver {

    String putObjectAcl(S3FileObjectPath s3FileObjectPath, AccessControlPolicy acl)
            throws S3Exception;
    AccessControlPolicy getObjectAcl(S3FileObjectPath s3FileObjectPath) throws S3Exception;
    void putBucketAcl(S3FileBucketPath s3FileBucketPath, AccessControlPolicy acl) throws S3Exception;
    AccessControlPolicy getBucketAcl(S3FileBucketPath s3FileBucketPath) throws S3Exception;
    AccessControlPolicy parseFromBytes(byte[] bytes) throws S3Exception;

}
