package com.thorinhood.acl;

import com.thorinhood.utils.XmlObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class AccessControlPolicy implements Serializable, XmlObject {

    private Owner owner;
    private List<Grant> accessControlList;
    private String xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";

    public static Builder builder() {
        return new Builder();
    }

    private AccessControlPolicy() {
    }

    public Owner getOwner() {
        return owner;
    }

    public List<Grant> getAccessControlList() {
        return accessControlList;
    }

    public String getXmlns() {
        return xmlns;
    }

    @Override
    public Element buildXmlRootNode(Document doc) {
        Element accessControlListNode = doc.createElement("AccessControlList");
        accessControlList.forEach(grant -> {
            accessControlListNode.appendChild(grant.buildXmlRootNode(doc));
        });
        Element root = createElement(doc, "AccessControlPolicy",
                owner != null ? createElement(doc, "Owner", owner.buildXmlRootNode(doc)) : null,
                accessControlList != null && !accessControlList.isEmpty() ? accessControlListNode : null);
        return appendAttributes(root, Map.of(
            "xmlns", xmlns
        ));
    }

    public static class Builder {
        private final AccessControlPolicy accessControlPolicy;

        public Builder() {
            accessControlPolicy = new AccessControlPolicy();
        }

        public Builder setXmlns(String xmlns) {
            accessControlPolicy.xmlns = xmlns;
            return this;
        }

        public Builder setOwner(Owner owner) {
            accessControlPolicy.owner = owner;
            return this;
        }

        public Builder setAccessControlList(List<Grant> accessControlList) {
            accessControlPolicy.accessControlList = accessControlList;
            return this;
        }

         public AccessControlPolicy build() {
            return accessControlPolicy;
         }

    }
}
