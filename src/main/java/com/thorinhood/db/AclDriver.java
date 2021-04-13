package com.thorinhood.db;

import com.thorinhood.acl.AccessControlPolicy;
import com.thorinhood.exceptions.S3Exception;

public interface AclDriver {

    boolean init() throws Exception;
    String putObjectAcl(String key, AccessControlPolicy acl) throws S3Exception;
    AccessControlPolicy getObjectAcl(String key) throws S3Exception;
    AccessControlPolicy parseFromBytes(byte[] bytes) throws S3Exception;

}
