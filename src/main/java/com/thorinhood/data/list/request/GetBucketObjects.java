package com.thorinhood.data.list.request;

public class GetBucketObjects {

    private String bucket;
    private String marker;
    private int maxKeys = 1000;
    private String prefix;
    private String delimiter;

    public static Builder builder() {
        return new Builder();
    }

    private GetBucketObjects() {
    }

    public String getBucket() {
        return bucket;
    }

    public String getMarker() {
        return marker;
    }

    public int getMaxKeys() {
        return maxKeys;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public static class Builder {
        private final GetBucketObjects getBucketObjects;

        public Builder() {
            getBucketObjects = new GetBucketObjects();
        }

        public Builder setBucket(String bucket) {
            getBucketObjects.bucket = bucket;
            return this;
        }

        public Builder setMarker(String marker) {
            getBucketObjects.marker = marker;
            return this;
        }

        public Builder setPrefix(String prefix) {
            getBucketObjects.prefix = prefix;
            return this;
        }

        public Builder setDelimiter(String delimiter) {
            getBucketObjects.delimiter = delimiter;
            return this;
        }

        public Builder setMaxKeys(int maxKeys) {
            getBucketObjects.maxKeys = maxKeys;
            return this;
        }

        public GetBucketObjects build() {
            return getBucketObjects;
        }
    }
}
