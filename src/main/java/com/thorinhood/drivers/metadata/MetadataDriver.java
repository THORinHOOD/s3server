package com.thorinhood.drivers.metadata;

import com.thorinhood.data.S3FileObjectPath;
import com.thorinhood.exceptions.S3Exception;

import java.util.Map;

public interface MetadataDriver {
    void putObjectMetadata(S3FileObjectPath s3FileObjectPath, Map<String, String> metadata, String eTag)
            throws S3Exception;
    Map<String, String> getObjectMetadata(S3FileObjectPath s3FileObjectPath) throws S3Exception;
}
