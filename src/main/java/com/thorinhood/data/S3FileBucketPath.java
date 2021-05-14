package com.thorinhood.data;

import com.thorinhood.exceptions.S3Exception;

import java.io.File;

public class S3FileBucketPath {

    protected final String baseFolderPath;
    protected final String bucket;

    public S3FileBucketPath(String baseFolderPath, String bucket) {
        this.baseFolderPath = baseFolderPath;
        this.bucket = bucket;
    }

    public String getBucket() {
        return bucket;
    }

    public String getPathToBucket() throws S3Exception {
        return baseFolderPath + File.separatorChar + bucket;
    }

    public String getPathToBucketMetadataFolder() {
        return baseFolderPath + File.separatorChar + S3FileStatic.METADATA_FOLDER_PREFIX + bucket;
    }

    public String getPathToBucketMetaFile() {
        return getPathToBucketMetadataFolder() + File.separatorChar + bucket + ".meta";
    }

    public String getPathToBucketAclFile() {
        return getPathToBucketMetadataFolder() + File.separatorChar + bucket + ".acl";
    }

    public String getPathToBucketPolicyFile() {
        return getPathToBucketMetadataFolder() + File.separatorChar + bucket + S3FileStatic.POSTFIX_POLICY_FILE;
    }

    public String getKeyWithBucket() {
        return bucket;
    }

    @Override
    public String toString() {
        return "S3FileBucketPath{" +
                "bucket='" + bucket + '\'' +
                '}';
    }
}
