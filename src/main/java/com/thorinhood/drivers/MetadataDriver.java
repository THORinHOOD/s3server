package com.thorinhood.drivers;

import com.thorinhood.exceptions.S3Exception;

import java.util.Map;

public interface MetadataDriver {

    boolean init() throws Exception;
    void setObjectMetadata(String key, Map<String, String> metadata) throws S3Exception;
    Map<String, String> getObjectMetadata(String key) throws S3Exception;

}
