package com.thorinhood.data.list.eventual;

import com.thorinhood.data.S3Content;
import com.thorinhood.utils.XmlObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ListBucketResultAbstract implements XmlObject {

    protected boolean isTruncated;
    protected List<S3Content> contents;
    protected String name;
    protected String prefix;
    protected String delimiter;
    protected int maxKeys = 1000;
    protected Set<String> commonPrefixes;
    protected String xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";

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

    public String getDelimiter() {
        return delimiter;
    }

    public int getMaxKeys() {
        return maxKeys;
    }

    public Set<String> getCommonPrefixes() {
        return commonPrefixes;
    }

    protected Element buildXmlContentsPrefixesAttr(Document doc, Element root) {
        if (contents != null && !contents.isEmpty()) {
            contents.forEach(content -> root.appendChild(content.buildXmlRootNode(doc)));
        }
        if (commonPrefixes != null && !commonPrefixes.isEmpty()) {
            Element commonPrefixesElement = doc.createElement("CommonPrefixes");
            commonPrefixes.forEach(prefix -> commonPrefixesElement
                    .appendChild(createTextElement(doc, "Prefix", prefix)));
            root.appendChild(commonPrefixesElement);
        }
        return appendAttributes(root, Map.of(
                "xmlns", xmlns
        ));
    }

    public static abstract class Builder<T extends Builder> {

        private ListBucketResultAbstract listBucketResultAbstract;
        private T childBuilder;

        protected void init(ListBucketResultAbstract listBucketResult, T childBuilder) {
            this.listBucketResultAbstract = listBucketResult;
            this.childBuilder = childBuilder;
        }

        public T setContents(List<S3Content> contents) {
            listBucketResultAbstract.contents = contents;
            return childBuilder;
        }

        public T setName(String name) {
            listBucketResultAbstract.name = name;
            return childBuilder;
        }

        public T setMaxKeys(int maxKeys) {
            listBucketResultAbstract.maxKeys = maxKeys;
            return childBuilder;
        }

        public T setPrefix(String prefix) {
            listBucketResultAbstract.prefix = prefix;
            return childBuilder;
        }

        public T setIsTruncated(boolean truncated) {
            listBucketResultAbstract.isTruncated = truncated;
            return childBuilder;
        }

        public T setDelimiter(String delimiter) {
            listBucketResultAbstract.delimiter = delimiter;
            return childBuilder;
        }

        public T setCommonPrefixes(Set<String> commonPrefixes) {
            listBucketResultAbstract.commonPrefixes = commonPrefixes;
            return childBuilder;
        }

    }
}
