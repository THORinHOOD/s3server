package com.thorinhood.data.s3object;

import java.util.Map;

public interface HasMetaData {
    S3Object setMetaData(Map<String, String> metadata);
    Map<String, String> getMetaData();
    String getKey();
}
