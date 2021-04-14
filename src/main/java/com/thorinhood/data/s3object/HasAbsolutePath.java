package com.thorinhood.data.s3object;

public interface HasAbsolutePath {
    HasKey setAbsolutePath(String absolutePath);
    String getAbsolutePath();
}
