package com.thorinhood.data;

public interface HasKey {
    HasETag setKey(String key);
    String getKey();
}
