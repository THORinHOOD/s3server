package com.thorinhood.acl;

import com.thorinhood.utils.XmlObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.Serializable;
import java.util.Map;

public class Grantee implements Serializable, XmlObject {

    enum Type {
        CanonicalUser, AmazonCustomerByEmail, Group;
    }

    private String displayName;
    private String emailAddress;
    private String id;
    private String type;
    private String xsi = "http://www.w3.org/2001/XMLSchema-instance";

    public static Grantee buildFromNode(Node node) {
        return new Builder().setFromRootNode(node).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private Grantee() {
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getEmailAddress() {
        return emailAddress;
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getXsi() {
        return xsi;
    }

    @Override
    public Element buildXmlRootNode(Document doc) {
        Element root = createElement(doc, "Grantee",
                displayName != null ? createElement(doc, "DisplayName", doc.createTextNode(displayName)) : null,
                emailAddress != null ? createElement(doc, "EmailAddress", doc.createTextNode(emailAddress)) : null,
                id != null ? createElement(doc, "ID", doc.createTextNode(id)) : null);
        return appendAttributes(root, Map.of(
                "xmlns:xsi", xsi,
                "xsi:type", type
        ));
    }


    public static class Builder {
        private final Grantee grantee;

        public Builder() {
            grantee = new Grantee();
        }

        public Builder setId(String id) {
            grantee.id = id;
            return this;
        }

        public Builder setType(String type) {
            grantee.type = type;
            return this;
        }

        public Builder setEmailAddress(String emailAddress) {
            grantee.emailAddress = emailAddress;
            return this;
        }

        public Builder setDisplayName(String displayName) {
            grantee.displayName = displayName;
            return this;
        }

        public Builder setXsi(String xsi) {
            grantee.xsi = xsi;
            return this;
        }

        public Builder setFromRootNode(Node node) {
            for (int i = 0; i < node.getChildNodes().getLength(); i++) {
                Node child = node.getChildNodes().item(i);
                if (child.getNodeName().equals("DisplayName")) {
                    grantee.displayName = child.getChildNodes().item(0).getNodeValue();
                } else if (child.getNodeName().equals("EmailAddress")) {
                    grantee.emailAddress = child.getChildNodes().item(0).getNodeValue();
                } else if (child.getNodeName().equals("ID")) {
                    grantee.id = child.getChildNodes().item(0).getNodeValue();
                }
            }
            grantee.type = node.getAttributes().getNamedItem("xsi:type").getNodeValue();
            grantee.xsi = node.getAttributes().getNamedItem("xmlns:xsi").getNodeValue();
            return this;
        }

        public Grantee build() {
            return grantee;
        }

    }
}
