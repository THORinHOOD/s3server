package com.thorinhood.data;

import com.thorinhood.exceptions.S3Exception;

import java.io.File;

public class S3ObjectPath extends S3BucketPath {

    private final String name;
    private final String subKey;

    public static S3ObjectPath raw(String bucket, String subKey, String name) {
        return new S3ObjectPath(bucket, subKey, name);
    }

    public static S3ObjectPath relative(String bucketKey) {
        int firstSlash = bucketKey.indexOf("/");
        String bucket = bucketKey.substring(0, firstSlash);
        bucketKey = bucketKey.substring(firstSlash + 1);
        int lastSlash = bucketKey.lastIndexOf("/");
        if (lastSlash == -1) {
            return new S3ObjectPath(bucket, "", bucketKey);
        }
        String name = bucketKey.substring(lastSlash + 1);
        bucketKey = bucketKey.substring(0, lastSlash + 1);
        return new S3ObjectPath(bucket, bucketKey, name);
    }

    private S3ObjectPath(String bucket, String subKey, String name) {
        super(bucket);
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
        if (subKey == null || name == null) {
            throw S3Exception.INTERNAL_ERROR("Can't get key of object : " + this)
                    .setMessage("Can't get key of object : " + this)
                    .setResource("1")
                    .setRequestId("1"); // TODO
        }
        return subKey + name;
    }

    public String getKeyWithBucket() throws S3Exception {
        String keyWithoutBucket = getKey();
        if (bucket == null) {
            throw S3Exception.INTERNAL_ERROR("Bucket name is null : " + this)
                    .setMessage("Bucket name is null : " + this)
                    .setResource("1")
                    .setRequestId("1"); // TODO
        }
        return bucket + File.separatorChar + keyWithoutBucket;
    }

    public String getFullPathToObject(String baseFolder) throws S3Exception {
        String path = getFullPathToObjectFolder(baseFolder);
        if (name == null) {
            throw S3Exception.INTERNAL_ERROR("Object name is null : " + this)
                    .setMessage("Object name is null : " + this)
                    .setResource("1")
                    .setRequestId("1");
        }
        return path + name;
    }

    public String getFullPathToObjectFolder(String baseFolder) throws S3Exception {
        String pathToBucket = getFullPathToBucket(baseFolder);
        if (subKey == null) {
            throw S3Exception.INTERNAL_ERROR("Can't build path to object folder : " + this)
                    .setMessage("Can't build path to object folder : " + this)
                    .setResource("1")
                    .setRequestId("1");
        }
        return pathToBucket + File.separatorChar + subKey;
    }

    public boolean isBucket() {
        return subKey == null && name == null;
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
