package com.thorinhood.acl;

import com.thorinhood.utils.XmlObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AccessControlPolicy implements Serializable, XmlObject {

    private Owner owner;
    private List<Grant> accessControlList;
    private String xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";

    public static AccessControlPolicy buildFromNode(Node node) {
        return new Builder().setFromRootNode(node).build();
    }

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
        if (accessControlList != null) {
            accessControlList.forEach(grant -> {
                accessControlListNode.appendChild(grant.buildXmlRootNode(doc));
            });
        }
        Element root = createElement(doc, "AccessControlPolicy",
                owner != null ? owner.buildXmlRootNode(doc) : null,
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

        public Builder setFromRootNode(Node node) {
            for (int i = 0; i < node.getChildNodes().getLength(); i++) {
                Node child = node.getChildNodes().item(i);
                if (child.getNodeName().equals("Owner")) {
                    accessControlPolicy.owner = Owner.buildFromNode(child);
                } else if (child.getNodeName().equals("AccessControlList")) {
                    List<Grant> grants = new ArrayList<>();
                    for (int j = 0; j < child.getChildNodes().getLength(); j++) {
                        if (child.getChildNodes().item(j).getNodeName().equals("Grant")) {
                                grants.add(Grant.buildFromNode(child.getChildNodes().item(j)));
                        }
                    }
                    accessControlPolicy.accessControlList = grants;
                }
            }
            accessControlPolicy.xmlns = node.getAttributes().getNamedItem("xmlns").getNodeValue();
            return this;
        }

        public AccessControlPolicy build() {
            return accessControlPolicy;
         }

    }
}
