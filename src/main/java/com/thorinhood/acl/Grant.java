package com.thorinhood.acl;

import com.thorinhood.utils.XmlObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.Serializable;

public class Grant implements Serializable, XmlObject {

    enum Permission {
        FULL_CONTROL, WRITE, WRITE_ACP, READ, READ_ACP;
    }

    private Grantee grantee;
    private String permission;

    public static Grant buildFromNode(Node node) {
        return new Builder().setFromRootNode(node).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private Grant() {
    }

    public Grantee getGrantee() {
        return grantee;
    }

    public String getPermission() {
        return permission;
    }

    @Override
    public Element buildXmlRootNode(Document doc) {
        return createElement(doc, "Grant",
                grantee.buildXmlRootNode(doc),
                createElement(doc, "Permission", doc.createTextNode(permission)));
    }

    public static class Builder {
        private final Grant grant;

        public Builder() {
            grant = new Grant();
        }

        public Builder setGrantee(Grantee grantee) {
            grant.grantee = grantee;
            return this;
        }

        public Builder setPermission(String permission) {
            grant.permission = permission;
            return this;
        }

        private Builder setFromRootNode(Node node) {
            for (int i = 0; i < node.getChildNodes().getLength(); i++) {
                Node child = node.getChildNodes().item(i);
                if (child.getNodeName().equals("Permission")) {
                    grant.permission = child.getChildNodes().item(0).getNodeValue();
                } else if (child.getNodeName().equals("Grantee")) {
                    grant.grantee = Grantee.buildFromNode(child);
                }
            }
            return this;
        }

        public Grant build() {
            return grant;
        }
    }
}
