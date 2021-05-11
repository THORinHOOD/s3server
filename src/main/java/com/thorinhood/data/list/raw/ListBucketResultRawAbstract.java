package com.thorinhood.data.list.raw;

import com.thorinhood.data.S3FileObjectPath;

import java.util.List;
import java.util.Set;

public abstract class ListBucketResultRawAbstract {

    protected List<S3FileObjectPath> s3FileObjectsPaths;
    protected boolean isTruncated;
    protected Set<String> commonPrefixes;

    public List<S3FileObjectPath> getS3FileObjectsPaths() {
        return s3FileObjectsPaths;
    }

    public boolean isTruncated() {
        return isTruncated;
    }

    public Set<String> getCommonPrefixes() {
        return commonPrefixes;
    }

    public static abstract class Builder<T extends Builder> {
        private ListBucketResultRawAbstract listBucketResultRawAbstract;
        private T childBuilder;

        protected void init(ListBucketResultRawAbstract listBucketResultRawAbstract, T childBuilder) {
            this.listBucketResultRawAbstract = listBucketResultRawAbstract;
            this.childBuilder = childBuilder;
        }

        public T setS3FileObjectsPaths(List<S3FileObjectPath> s3FileObjectsPaths) {
            listBucketResultRawAbstract.s3FileObjectsPaths = s3FileObjectsPaths;
            return childBuilder;
        }

        public T setIsTruncated(boolean isTruncated) {
            listBucketResultRawAbstract.isTruncated = isTruncated;
            return childBuilder;
        }

        public T setCommonPrefixes(Set<String> commonPrefixes) {
            listBucketResultRawAbstract.commonPrefixes = commonPrefixes;
            return childBuilder;
        }
    }
}
