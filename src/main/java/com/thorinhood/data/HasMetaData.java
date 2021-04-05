package com.thorinhood.data;

import java.util.Map;

public interface HasMetaData {
    S3Object setMetaData(Map<String, String> metadata);
    Map<String, String> getMetaData();
}
