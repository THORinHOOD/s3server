package com.thorinhood.data;

import java.io.*;

public class S3Object {

    private String absolutePath;
    private String key;
    private String ETag;
    private File file;
    private byte[] bytes;
    private String lastModified;

    public String getLastModified() {
        return lastModified;
    }

    public String getAbsolutePath() {
        return absolutePath;
    }

    public String getKey() {
        return key;
    }

    public String getETag() {
        return ETag;
    }

    public File getFile() {
        return file;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public S3Object setAbsolutePath(String absolutePath) {
        this.absolutePath = absolutePath;
        return this;
    }

    public S3Object setKey(String key) {
        this.key = key;
        return this;
    }

    public S3Object setETag(String ETag) {
        this.ETag = ETag;
        return this;
    }

    public S3Object setFile(File file) {
        this.file = file;
        return this;
    }

    public S3Object setBytes(byte[] bytes) {
        this.bytes = bytes;
        return this;
    }

    public S3Object setLastModified(String lastModified) {
        this.lastModified = lastModified;
        return this;
    }
}
