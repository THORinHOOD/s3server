package com.thorinhood.data.s3object;

public interface HasETag {
    HasFile setETag(String ETag);
    String getETag();
}
