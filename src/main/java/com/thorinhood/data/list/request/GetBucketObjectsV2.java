package com.thorinhood.data.list.request;

public class GetBucketObjectsV2 {

    private String bucket;
    private String prefix;
    private String startAfter;
    private String continuationToken;
    private String delimiter;
    private int maxKeys = 1000;

    public static Builder builder() {
        return new Builder();
    }

    private GetBucketObjectsV2() {
    }

    public String getPrefix() {
        return prefix;
    }

    public String getBucket() {
        return bucket;
    }

    public int getMaxKeys() {
        return maxKeys;
    }

    public String getStartAfter() {
        return startAfter;
    }

    public String getContinuationToken() {
        return continuationToken;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public static class Builder {
        private final GetBucketObjectsV2 request;

        public Builder() {
            this.request = new GetBucketObjectsV2();
        }

        public Builder setBucket(String bucket) {
            request.bucket = bucket;
            return this;
        }

        public Builder setMaxKeys(int maxKeys) {
            request.maxKeys = maxKeys;
            return this;
        }

        public Builder setPrefix(String prefix) {
            request.prefix = prefix;
            return this;
        }

        public Builder setStartAfter(String startAfter) {
            request.startAfter = startAfter;
            return this;
        }

        public Builder setContinuationToken(String continuationToken) {
            request.continuationToken = continuationToken;
            return this;
        }

        public Builder setDelimiter(String delimiter) {
            request.delimiter = delimiter;
            return this;
        }

        public GetBucketObjectsV2 build() {
            return request;
        }
    }

}
