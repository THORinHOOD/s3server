package com.thorinhood.data.list.eventual;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ListBucketV2Result extends ListBucketResultAbstract {

    private String startAfter;
    private int keyCount;
    private String nextContinuationToken;
    private String continuationToken;

    public static Builder builder() {
        return new Builder();
    }

    private ListBucketV2Result() {
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
                            createTextElement(doc, "ContinuationToken", continuationToken),
                            createTextElement(doc, "Delimiter", delimiter));
        return buildXmlContentsPrefixesAttr(doc, root);
    }

    public String getStartAfter() {
        return startAfter;
    }

    public int getKeyCount() {
        return keyCount;
    }

    public String getNextContinuationToken() {
        return nextContinuationToken;
    }

    public String getContinuationToken() {
        return continuationToken;
    }

    public static class Builder extends ListBucketResultAbstract.Builder<Builder> {

        private final ListBucketV2Result listBucketV2Result;

        public Builder() {
            listBucketV2Result = new ListBucketV2Result();
            init(listBucketV2Result, this);
        }

        public Builder setStartAfter(String startAfter) {
            listBucketV2Result.startAfter = startAfter;
            return this;
        }

        public Builder setKeyCount(int keyCount) {
            listBucketV2Result.keyCount = keyCount;
            return this;
        }

        public Builder setNextContinuationToken(String nextContinuationToken) {
            listBucketV2Result.nextContinuationToken = nextContinuationToken;
            return this;
        }

        public Builder setContinuationToken(String continuationToken) {
            listBucketV2Result.continuationToken = continuationToken;
            return this;
        }

        public ListBucketV2Result build() {
            return listBucketV2Result;
        }
    }

}
