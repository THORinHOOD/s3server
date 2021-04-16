package com.thorinhood.drivers.acl;

import com.thorinhood.data.acl.AccessControlPolicy;
import com.thorinhood.exceptions.S3Exception;

public interface AclDriver {

    String putObjectAcl(String bucket, String key, AccessControlPolicy acl) throws S3Exception;
    AccessControlPolicy getObjectAcl(String bucket, String key) throws S3Exception;
    void putBucketAcl(String bucket, AccessControlPolicy acl) throws S3Exception;
    AccessControlPolicy getBucketAcl(String bucket) throws S3Exception;
    AccessControlPolicy parseFromBytes(byte[] bytes) throws S3Exception;

}
