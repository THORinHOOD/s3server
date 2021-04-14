package com.thorinhood.drivers.acl;

import com.thorinhood.acl.AccessControlPolicy;
import com.thorinhood.exceptions.S3Exception;

public interface AclDriver {

    boolean init() throws Exception;
    String putObjectAcl(String key, AccessControlPolicy acl) throws S3Exception;
    AccessControlPolicy getObjectAcl(String key) throws S3Exception;
    void putBucketAcl(String basePath, String bucket, AccessControlPolicy acl) throws S3Exception;
    AccessControlPolicy getBucketAcl(String basePath, String bucket) throws S3Exception;
    AccessControlPolicy parseFromBytes(byte[] bytes) throws S3Exception;

}
