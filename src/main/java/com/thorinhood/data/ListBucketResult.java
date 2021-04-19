package com.thorinhood.data;

import com.thorinhood.utils.XmlObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.List;

public class ListBucketResult implements XmlObject {

//    private boolean isTruncated;
//    private String marker;
//    private String nextMarker;
    private List<S3Content> contents;
    private String name;
//    private String prefix;
//    private String delimiter;
    private int maxKeys = 1000;
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
                            createTextElement(doc, "MaxKeys", String.valueOf(maxKeys)));
        if (contents != null && !contents.isEmpty()) {
            contents.forEach(content -> root.appendChild(content.buildXmlRootNode(doc)));
        }
        return root;
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

        public ListBucketResult build() {
            return listBucketResult;
        }
    }

}
