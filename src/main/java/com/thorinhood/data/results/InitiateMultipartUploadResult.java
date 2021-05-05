package com.thorinhood.data.results;

import com.thorinhood.utils.XmlObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class InitiateMultipartUploadResult implements XmlObject {

    private String bucket;
    private String key;
    private String uploadId;

    public static Builder builder() {
        return new Builder();
    }

    private InitiateMultipartUploadResult() {
    }

    @Override
    public Element buildXmlRootNode(Document doc) {
        return createElement(doc, "InitiateMultipartUploadResult",
                createTextElement(doc, "Bucket", bucket),
                createTextElement(doc, "Key", key),
                createTextElement(doc, "UploadId", uploadId));
    }

    public static class Builder {

        private final InitiateMultipartUploadResult initiateMultipartUploadResult;

        public Builder() {
            initiateMultipartUploadResult = new InitiateMultipartUploadResult();
        }

        public Builder setBucket(String bucket) {
            initiateMultipartUploadResult.bucket = bucket;
            return this;
        }

        public Builder setKey(String key) {
            initiateMultipartUploadResult.key = key;
            return this;
        }

        public Builder setUploadId(String uploadId) {
            initiateMultipartUploadResult.uploadId = uploadId;
            return this;
        }

        public InitiateMultipartUploadResult build() {
            return initiateMultipartUploadResult;
        }
    }
}
