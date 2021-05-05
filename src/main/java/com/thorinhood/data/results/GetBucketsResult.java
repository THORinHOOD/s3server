package com.thorinhood.data.results;

import com.thorinhood.data.Owner;
import com.thorinhood.utils.Pair;
import com.thorinhood.utils.XmlObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.List;

public class GetBucketsResult implements XmlObject {

    private List<Pair<String, String>> buckets;
    private Owner owner;

    public static Builder builder() {
        return new Builder();
    }

    private GetBucketsResult() {
    }

    @Override
    public Element buildXmlRootNode(Document doc) {
        Element root = createElement(doc, "ListAllMyBucketsResult",
                    owner.buildXmlRootNode(doc));
        Element bucketsElement = doc.createElement("Buckets");
        if (buckets != null) {
            buckets.forEach(bucket -> {
                Element bucketElement = createElement(doc, "Bucket",
                        createTextElement(doc, "CreationDate", bucket.getSecond()),
                        createTextElement(doc, "Name", bucket.getFirst()));
                bucketsElement.appendChild(bucketElement);
            });
        }
        root.appendChild(bucketsElement);
        return root;
    }

    public List<Pair<String, String>> getBuckets() {
        return buckets;
    }

    public Owner getOwner() {
        return owner;
    }

    public static class Builder {

        private final GetBucketsResult getBucketsResult;

        public Builder() {
            getBucketsResult = new GetBucketsResult();
        }

        public Builder setBuckets(List<Pair<String, String>> buckets) {
            getBucketsResult.buckets = buckets;
            return this;
        }

        public Builder setOwner(Owner owner) {
            getBucketsResult.owner = owner;
            return this;
        }

        public GetBucketsResult build() {
            return getBucketsResult;
        }
    }

}
