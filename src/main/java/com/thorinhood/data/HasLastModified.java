package com.thorinhood.data;

public interface HasLastModified {
    HasMetaData setLastModified(String lastModified);
    String getLastModified();
}
