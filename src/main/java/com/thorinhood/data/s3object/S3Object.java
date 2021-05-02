package com.thorinhood.data.s3object;

import com.thorinhood.data.S3ObjectPath;

import java.io.File;
import java.util.Map;

public class S3Object implements HasAbsolutePath, HasS3Path, HasETag, HasFile, HasRawBytes, HasLastModified, HasMetaData {

    private String absolutePath;
    private S3ObjectPath s3ObjectPath;
    private String ETag;
    private File file;
    private byte[] bytes;
    private String lastModified;
    private Map<String, String> metadata;

    private S3Object() {
    }

    public static HasAbsolutePath build() {
        return new S3Object();
    }

    @Override
    public HasS3Path setAbsolutePath(String absolutePath) {
        this.absolutePath = absolutePath;
        return this;
    }

    @Override
    public HasFile setETag(String ETag) {
        this.ETag = ETag;
        return this;
    }

    @Override
    public HasRawBytes setFile(File file) {
        this.file = file;
        return this;
    }

    @Override
    public HasETag setS3Path(S3ObjectPath s3ObjectPath) {
        this.s3ObjectPath = s3ObjectPath;
        return this;
    }

    @Override
    public HasLastModified setRawBytes(byte[] rawBytes) {
        bytes = rawBytes;
        return this;
    }

    @Override
    public HasMetaData setLastModified(String lastModified) {
        this.lastModified = lastModified;
        return this;
    }

    @Override
    public String getAbsolutePath() {
        return absolutePath;
    }

    @Override
    public String getETag() {
        return ETag;
    }

    @Override
    public File getFile() {
        return file;
    }

    @Override
    public S3ObjectPath getS3Path() {
        return s3ObjectPath;
    }

    @Override
    public String getLastModified() {
        return lastModified;
    }

    @Override
    public byte[] getRawBytes() {
        return bytes;
    }

    @Override
    public S3Object setMetaData(Map<String, String> metadata) {
        this.metadata = metadata;
        return this;
    }

    @Override
    public Map<String, String> getMetaData() {
        return metadata;
    }
}
