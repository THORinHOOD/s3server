package com.thorinhood.data;

import com.thorinhood.utils.XmlObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.Serializable;

public class Owner implements Serializable, XmlObject {

    private String id;
    private String displayName;

    public static Owner buildFromNode(Node node) {
        return new Builder().setFromRootNode(node).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private Owner() {
    }

    public String getId() {
        return id;
    }

    public String getDisplayName() {
        return displayName;
    }

    @Override
    public Element buildXmlRootNode(Document doc) {
        return createElement(doc, "Owner",
                id != null ? createElement(doc, "ID", doc.createTextNode(id)) : null,
                displayName != null ? createElement(doc, "DisplayName", doc.createTextNode(displayName)) : null);
    }

    public static class Builder {
        private final Owner owner;

        public Builder() {
            owner = new Owner();
        }

        public Builder setId(String id) {
            owner.id = id;
            return this;
        }

        public Builder setDisplayName(String displayName) {
            owner.displayName = displayName;
            return this;
        }

        private Builder setFromRootNode(Node node) {
            for (int i = 0; i < node.getChildNodes().getLength(); i++) {
                setFromNode(node.getChildNodes().item(i));
            }
            return this;
        }

        private void setFromNode(Node node) {
            if (node.getNodeName().equals("ID")) {
                owner.id = node.getChildNodes().item(0).getNodeValue();
            } else if (node.getNodeName().equals("DisplayName")) {
                owner.displayName = node.getChildNodes().item(0).getNodeValue();
            }
        }

        public Owner build() {
            return owner;
        }
    }
}
