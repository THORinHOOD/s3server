package com.thorinhood.data;

import org.apache.commons.codec.digest.DigestUtils;

import java.util.Objects;

public class S3ETag {

    private final String eTag;

    public static S3ETag fromBytes(byte[] bytes) {
        return new S3ETag(DigestUtils.md5Hex(bytes));
    }

    public S3ETag(String eTag) {
        this.eTag = toCanonical(eTag);
    }

    public String get() {
        return eTag;
    }

    private String toCanonical(String eTag) {
        if (!eTag.startsWith("\"") && !eTag.endsWith("\"")) {
            return "\"" + eTag + "\"";
        } else {
            return eTag;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        S3ETag s3ETag = (S3ETag) o;
        return Objects.equals(eTag, s3ETag.eTag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eTag);
    }

}
