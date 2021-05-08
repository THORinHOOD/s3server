package com.thorinhood.data.s3object;

import com.thorinhood.data.S3FileObjectPath;

import java.io.File;

public class S3ObjectETag {

    private final String eTag;
    private final S3FileObjectPath s3FileObjectPath;
    private final File file;

    public S3ObjectETag(String eTag, S3FileObjectPath s3FileObjectPath) {
        this.eTag = eTag;
        this.s3FileObjectPath = s3FileObjectPath;
        file = new File(s3FileObjectPath.getPathToObject());
    }

    public String getETag() {
        return eTag;
    }

    public S3FileObjectPath getS3FileObjectPath() {
        return s3FileObjectPath;
    }

    public File getFile() {
        return file;
    }
}
