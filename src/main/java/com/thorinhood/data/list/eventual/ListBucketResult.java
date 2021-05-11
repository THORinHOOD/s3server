package com.thorinhood.data.list.eventual;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ListBucketResult extends ListBucketResultAbstract {

    private String marker;
    private String nextMarker;

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
                createTextElement(doc, "IsTruncated", String.valueOf(isTruncated)),
                createTextElement(doc, "Marker", marker),
                createTextElement(doc, "NextMarker", nextMarker),
                createTextElement(doc, "Delimiter", delimiter));
        return buildXmlContentsPrefixesAttr(doc, root);
    }

    public String getMarker() {
        return marker;
    }

    public String getNextMarker() {
        return nextMarker;
    }

    public static class Builder extends ListBucketResultAbstract.Builder<Builder> {

        private final ListBucketResult listBucketResult;

        public Builder() {
            listBucketResult = new ListBucketResult();
            init(listBucketResult, this);
        }

        public Builder setMarker(String marker) {
            listBucketResult.marker = marker;
            return this;
        }

        public Builder setNextMarker(String nextMarket) {
            listBucketResult.nextMarker = nextMarket;
            return this;
        }

        public ListBucketResult build() {
            return listBucketResult;
        }
    }

}
