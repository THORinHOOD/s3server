package com.thorinhood.data;

public interface HasRawBytes {
    HasLastModified setRawBytes(byte[] rawBytes);
    byte[] getRawBytes();
}
