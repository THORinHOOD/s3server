package com.thorinhood.drivers.metadata;

import com.thorinhood.data.S3ObjectPath;
import com.thorinhood.drivers.lock.PreparedOperationFileWrite;
import com.thorinhood.exceptions.S3Exception;

import java.util.Map;

public interface MetadataDriver {
    PreparedOperationFileWrite putObjectMetadata(S3ObjectPath s3ObjectPath, Map<String, String> metadata)
            throws S3Exception;
    Map<String, String> getObjectMetadata(S3ObjectPath s3ObjectPath) throws S3Exception;
}
