package com.thorinhood.drivers.metadata;

import com.thorinhood.exceptions.S3Exception;

import java.util.Map;

public interface MetadataDriver {

    void setObjectMetadata(String bucket, String key, Map<String, String> metadata) throws S3Exception;
    Map<String, String> getObjectMetadata(String bucket, String key) throws S3Exception;

}
