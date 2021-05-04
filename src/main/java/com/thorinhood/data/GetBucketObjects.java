package com.thorinhood.data;

public class GetBucketObjects {

    private String bucket;
    private String prefix;
    private String startAfter;
    private String continuationToken;
    private int maxKeys = 1000;

    public static Builder builder() {
        return new Builder();
    }

    private GetBucketObjects() {
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

    public static class Builder {
        private final GetBucketObjects request;

        public Builder() {
            this.request = new GetBucketObjects();
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

        public GetBucketObjects build() {
            return request;
        }
    }

}
