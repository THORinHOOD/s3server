package com.thorinhood.data;

import com.thorinhood.exceptions.S3Exception;

import java.io.File;

public class S3FileObjectPath extends S3FileBucketPath {

    private String name;
    private String subKey;



    public static S3FileObjectPath raw(String baseFolderPath, String bucket, String subKey, String name) {
        return new S3FileObjectPath(baseFolderPath, bucket, subKey, name);
    }
    public static S3FileObjectPath raw(String baseFolderPath, String bucket, String key) {
        return relative(baseFolderPath, bucket + File.separatorChar + key);
    }

    public static S3FileObjectPath relative(String baseFolderPath, String bucketKey) {
        int firstSlash = bucketKey.indexOf(File.separatorChar);
        String bucket = bucketKey.substring(0, firstSlash);
        bucketKey = bucketKey.substring(firstSlash + 1);
        int lastSlash = bucketKey.lastIndexOf(File.separatorChar);
        if (lastSlash == -1) {
            return new S3FileObjectPath(baseFolderPath, bucket, "", bucketKey);
        }
        String name = bucketKey.substring(lastSlash + 1);
        bucketKey = bucketKey.substring(0, lastSlash + 1);
        return new S3FileObjectPath(baseFolderPath, bucket, bucketKey, name);
    }

    public S3FileObjectPath(String baseFolderPath, String bucket, String subKey, String name) {
        super(baseFolderPath, bucket);
        this.subKey = subKey;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getSubKey() {
        return subKey;
    }

    public String getKeyUnsafe() throws S3Exception {
        if (subKey == null || name == null) {
            return null;
        }
        return subKey + name;
    }

    public String getKey() throws S3Exception {
        return subKey + name;
    }

    public String getKeyWithBucket() throws S3Exception {
        return bucket + File.separatorChar + getKey();
    }

    public String getPathToObject() throws S3Exception {
        return getPathToObjectFolder() + name;
    }

    public String getPathToObjectFolder() throws S3Exception {
        return getPathToBucket() + File.separatorChar + subKey;
    }

    public S3FileObjectPath appendToSubKey(String toAppend) {
        subKey += toAppend;
        return this;
    }

    public S3FileObjectPath setName(String name) {
        this.name = name;
        return this;
    }

    public boolean isBucket() {
        return subKey == null && name == null;
    }

    public String getPathToObjectMetadataFolder() throws S3Exception {
        return getPathToObjectFolder() + S3FileStatic.METADATA_FOLDER_PREFIX + name;
    }

    public String getPathToObjectMetaFile() {
        return getPathToObjectMetadataFolder() + File.separatorChar + name + ".meta";
    }

    public String getPathToObjectAclFile() {
        return getPathToObjectMetadataFolder() + File.separatorChar + name + ".acl";
    }

    public String getPathToObjectUploadFolder(String uploadId) {
        return getPathToObjectMultipartFolder() + File.separatorChar + uploadId;
    }

    public String getPathToObjectMultipartFolder() {
        return getPathToObjectMetadataFolder() + File.separatorChar + S3FileStatic.MULTIPART_FOLDER_NAME;
    }

    public String getPathToObjectUploadPart(String uploadId, int partNumber) {
       return getPathToObjectUploadFolder(uploadId) + File.separatorChar + partNumber;
    }

    @Override
    public String toString() {
        return "S3Path{" +
                " bucket='" + bucket + '\'' +
                ", subKey='" + subKey + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
