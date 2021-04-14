package com.thorinhood.data.s3object;

public interface HasKey {
    HasETag setKey(String key);
    String getKey();
}
