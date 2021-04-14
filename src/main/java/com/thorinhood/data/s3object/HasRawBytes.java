package com.thorinhood.data.s3object;

public interface HasRawBytes {
    HasLastModified setRawBytes(byte[] rawBytes);
    byte[] getRawBytes();
}
