package com.thorinhood.data;

public interface HasETag {
    HasFile setETag(String ETag);
    String getETag();
}
