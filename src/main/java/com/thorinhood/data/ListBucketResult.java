package com.thorinhood.data;

import com.thorinhood.utils.XmlObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.List;

public class ListBucketResult implements XmlObject {

    private boolean isTruncated;
//    private String marker;
//    private String nextMarker;
    private List<S3Content> contents;
    private String name;
    private String prefix;
    private String startAfter;
    private int keyCount;
//    private String delimiter;
    private int maxKeys = 1000;
    private String nextContinuationToken;
    private String continuationToken;
//    private List<String> commonPrefixes;
//    private String encodingType;

    public static Builder builder() {
        return new Builder();
    }

    private ListBucketResult() {
    }

    @Override
    public Element buildXmlRootNode(Document doc) {
        Element root = createElement(doc, "ListBucketResult",
                            createTextElement(doc, "Name", name),
                            createTextElement(doc, "MaxKeys", String.valueOf(maxKeys)),
                            createTextElement(doc, "Prefix", prefix),
                            createTextElement(doc, "StartAfter", startAfter),
                            createTextElement(doc, "IsTruncated", String.valueOf(isTruncated)),
                            createTextElement(doc, "KeyCount", String.valueOf(keyCount)),
                            createTextElement(doc, "NextContinuationToken", nextContinuationToken),
                            createTextElement(doc, "ContinuationToken", continuationToken));
        if (contents != null && !contents.isEmpty()) {
            contents.forEach(content -> root.appendChild(content.buildXmlRootNode(doc)));
        }
        return root;
    }

    public boolean isTruncated() {
        return isTruncated;
    }

    public List<S3Content> getContents() {
        return contents;
    }

    public String getName() {
        return name;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getStartAfter() {
        return startAfter;
    }

    public int getKeyCount() {
        return keyCount;
    }

    public int getMaxKeys() {
        return maxKeys;
    }

    public String getNextContinuationToken() {
        return nextContinuationToken;
    }

    public String getContinuationToken() {
        return continuationToken;
    }

    public static class Builder {

        private final ListBucketResult listBucketResult;

        public Builder() {
            listBucketResult = new ListBucketResult();
        }

        public Builder setContents(List<S3Content> contents) {
            listBucketResult.contents = contents;
            return this;
        }

        public Builder setName(String name) {
            listBucketResult.name = name;
            return this;
        }

        public Builder setMaxKeys(int maxKeys) {
            listBucketResult.maxKeys = maxKeys;
            return this;
        }

        public Builder setPrefix(String prefix) {
            listBucketResult.prefix = prefix;
            return this;
        }

        public Builder setStartAfter(String startAfter) {
            listBucketResult.startAfter = startAfter;
            return this;
        }

        public Builder setIsTruncated(boolean truncated) {
            listBucketResult.isTruncated = truncated;
            return this;
        }

        public Builder setKeyCount(int keyCount) {
            listBucketResult.keyCount = keyCount;
            return this;
        }

        public Builder setNextContinuationToken(String nextContinuationToken) {
            listBucketResult.nextContinuationToken = nextContinuationToken;
            return this;
        }

        public Builder setContinuationToken(String continuationToken) {
            listBucketResult.continuationToken = continuationToken;
            return this;
        }

        public ListBucketResult build() {
            return listBucketResult;
        }
    }

}
