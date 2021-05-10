package com.thorinhood.data.results;

import com.thorinhood.utils.XmlObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class CopyObjectResult implements XmlObject {

    private String lastModified;
    private String eTag;

    public static Builder builder() {
        return new Builder();
    }

    private CopyObjectResult() {
    }

    @Override
    public Element buildXmlRootNode(Document doc) {
        return createElement(doc, "CopyObjectResult",
                createTextElement(doc, "LastModified", lastModified),
                createTextElement(doc, "ETag", eTag));
    }

    public static class Builder {
        private final CopyObjectResult copyObjectResult;

        public Builder() {
            copyObjectResult = new CopyObjectResult();
        }

        public Builder setLastModified(String lastModified) {
            copyObjectResult.lastModified = lastModified;
            return this;
        }

        public Builder setETag(String eTag) {
            copyObjectResult.eTag = eTag;
            return this;
        }

        public CopyObjectResult build() {
            return copyObjectResult;
        }
    }

}
