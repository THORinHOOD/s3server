package com.thorinhood.data.s3object;

public interface HasAbsolutePath {
    HasS3Path setAbsolutePath(String absolutePath);
    String getAbsolutePath();
}
