package com.thorinhood.data.results;

import com.thorinhood.utils.XmlObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.Map;

public class CompleteMultipartUploadResult implements XmlObject {

    private String location;
    private String bucket;
    private String key;
    private String eTag;
    private String xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";

    public static Builder builder() {
        return new Builder();
    }

    private CompleteMultipartUploadResult() {
    }

    @Override
    public Element buildXmlRootNode(Document doc) {
        Element root = createElement(doc, "CompleteMultipartUploadResult",
                createTextElement(doc, "Location", location),
                createTextElement(doc, "Bucket", bucket),
                createTextElement(doc, "Key", key),
                createTextElement(doc, "ETag", eTag));
        return appendAttributes(root, Map.of(
                "xmlns", xmlns
        ));
    }

    public static class Builder {

        private final CompleteMultipartUploadResult completeMultipartUploadResult;

        public Builder() {
            completeMultipartUploadResult = new CompleteMultipartUploadResult();
        }

        public Builder setLocation(String location) {
            completeMultipartUploadResult.location = location;
            return this;
        }

        public Builder setBucket(String bucket) {
            completeMultipartUploadResult.bucket = bucket;
            return this;
        }

        public Builder setKey(String key) {
            completeMultipartUploadResult.key = key;
            return this;
        }

        public Builder setETag(String eTag) {
            completeMultipartUploadResult.eTag = eTag;
            return this;
        }

        public CompleteMultipartUploadResult build() {
            return completeMultipartUploadResult;
        }
    }

}
