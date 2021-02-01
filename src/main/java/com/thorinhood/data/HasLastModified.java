package com.thorinhood.data;

public interface HasLastModified {
    S3Object setLastModified(String lastModified);
    String getLastModified();
}
