package com.thorinhood.data.s3object;

import com.thorinhood.data.S3ObjectPath;

import java.util.Map;

public interface HasMetaData {
    S3Object setMetaData(Map<String, String> metadata);
    Map<String, String> getMetaData();
    S3ObjectPath getS3Path();
}
