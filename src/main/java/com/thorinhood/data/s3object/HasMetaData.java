package com.thorinhood.data.s3object;

import com.thorinhood.data.S3FileObjectPath;

import java.util.Map;

public interface HasMetaData {
    S3Object setMetaData(Map<String, String> metadata);
    Map<String, String> getMetaData();
    S3FileObjectPath getS3Path();
}
