package com.thorinhood.data;

public class GetBucketObjectsRequest {

    private String bucket;
    private String prefix;
    private String marker;
    private int maxKeys = 1000;

    public static Builder builder() {
        return new Builder();
    }

    private GetBucketObjectsRequest() {
    }

    public String getMarker() {
        return marker;
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

    public static class Builder {
        private final GetBucketObjectsRequest request;

        public Builder() {
            this.request = new GetBucketObjectsRequest();
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

        public Builder setMarker(String marker) {
            request.marker = marker;
            return this;
        }

        public GetBucketObjectsRequest build() {
            return request;
        }
    }

}
