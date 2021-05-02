package com.thorinhood.data;

import com.thorinhood.exceptions.S3Exception;

import java.io.File;

public class S3BucketPath {

    protected final String bucket;

    public static S3BucketPath build(String bucket) {
        return new S3BucketPath(bucket);
    }

    protected S3BucketPath(String bucket) {
        this.bucket = bucket;
    }

    public String getBucket() {
        return bucket;
    }

    public String getFullPathToBucket(String baseFolder) throws S3Exception {
        if (bucket == null) {
            throw S3Exception.INTERNAL_ERROR("Bucket name is null")
                    .setMessage("Bucket name is null")
                    .setResource("1")
                    .setRequestId("1");
        }
        return baseFolder + File.separatorChar + bucket;
    }

}
