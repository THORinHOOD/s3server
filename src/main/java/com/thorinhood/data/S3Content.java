package com.thorinhood.data;

import com.thorinhood.utils.XmlObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class S3Content implements XmlObject {

    private String key;
    private String lastModified;
    private String eTag;
    private long size;
    private Owner owner;
    private String storageClass;

    public static Builder builder() {
        return new Builder();
    }

    private S3Content() {
    }

    @Override
    public Element buildXmlRootNode(Document doc) {
        return createElement(doc, "Contents",
                    createTextElement(doc, "Key", key),
                    createTextElement(doc, "LastModified", lastModified),
                    createTextElement(doc, "ETag", "\"" + eTag + "\""),
                    createTextElement(doc, "Size", String.valueOf(size)),
                    createTextElement(doc, "StorageClass", storageClass),
                    owner != null ? owner.buildXmlRootNode(doc) : null);
    }

    public static class Builder {

        private final S3Content s3Content;

        public Builder() {
            s3Content = new S3Content();
        }

        public Builder setKey(String key) {
            s3Content.key = key;
            return this;
        }

        public Builder setLastModified(String lastModified) {
            s3Content.lastModified = lastModified;
            return this;
        }

        public Builder setETag(String eTag) {
            s3Content.eTag = eTag;
            return this;
        }

        public Builder setSize(long size) {
            s3Content.size = size;
            return this;
        }

        public Builder setOwner(Owner owner) {
            s3Content.owner = owner;
            return this;
        }

        public Builder setStorageClass(String storageClass) {
            s3Content.storageClass = storageClass;
            return this;
        }

        public S3Content build() {
            return s3Content;
        }
    }
}
