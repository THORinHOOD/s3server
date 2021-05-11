package com.thorinhood.data.list.raw;

public class ListBucketV2ResultRaw extends ListBucketResultRawAbstract {

    private String nextContinuationToken;
    private int keyCount;

    public static Builder builder() {
        return new Builder();
    }

    private ListBucketV2ResultRaw(){
    }

    public String getNextContinuationToken() {
        return nextContinuationToken;
    }

    public int getKeyCount() {
        return keyCount;
    }

    public static class Builder extends ListBucketResultRawAbstract.Builder<Builder> {
        private final ListBucketV2ResultRaw listBucketV2ResultRaw;

        public Builder() {
            listBucketV2ResultRaw = new ListBucketV2ResultRaw();
            init(listBucketV2ResultRaw, this);
        }

        public Builder setNextContinuationToken(String nextContinuationToken) {
            listBucketV2ResultRaw.nextContinuationToken = nextContinuationToken;
            return this;
        }

        public Builder setKeyCount(int keyCount) {
            listBucketV2ResultRaw.keyCount = keyCount;
            return this;
        }

        public ListBucketV2ResultRaw build() {
            return listBucketV2ResultRaw;
        }
    }
}
