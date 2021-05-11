package com.thorinhood.data.list.raw;

public class ListBucketResultRaw extends ListBucketResultRawAbstract {

    private String nextMarker;

    public static Builder builder() {
        return new Builder();
    }

    private ListBucketResultRaw() {
    }

    public String getNextMarker() {
        return nextMarker;
    }

    public static class Builder extends ListBucketResultRawAbstract.Builder<Builder> {
        private final ListBucketResultRaw listBucketResultRaw;

        public Builder() {
            listBucketResultRaw = new ListBucketResultRaw();
            init(listBucketResultRaw, this);
        }

        public Builder setNextMarker(String nextMarker) {
            listBucketResultRaw.nextMarker = nextMarker;
            return this;
        }

        public ListBucketResultRaw build() {
            return listBucketResultRaw;
        }
    }
}
