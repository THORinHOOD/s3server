package com.thorinhood.data;

public class GetBucketObjectsRequest {

    private String bucket;
    private int maxKeys = 1000;

    public static Builder builder() {
        return new Builder();
    }

    private GetBucketObjectsRequest() {
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

        public GetBucketObjectsRequest build() {
            return request;
        }
    }

}
