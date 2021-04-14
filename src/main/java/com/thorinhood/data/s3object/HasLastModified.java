package com.thorinhood.data.s3object;

public interface HasLastModified {
    HasMetaData setLastModified(String lastModified);
    String getLastModified();
}
