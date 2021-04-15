package com.thorinhood.data.acl;

import com.thorinhood.utils.XmlObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.Serializable;

public class Grant implements Serializable, XmlObject {

    private Grantee grantee;
    private Permission permission;

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

    public Permission getPermission() {
        return permission;
    }

    @Override
    public Element buildXmlRootNode(Document doc) {
        return createElement(doc, "Grant",
                grantee != null ? grantee.buildXmlRootNode(doc) : null,
                permission != null ? createElement(doc, "Permission", doc.createTextNode(permission.name())) : null);
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

        public Builder setPermission(Permission permission) {
            grant.permission = permission;
            return this;
        }

        private Builder setFromRootNode(Node node) {
            for (int i = 0; i < node.getChildNodes().getLength(); i++) {
                Node child = node.getChildNodes().item(i);
                if (child.getNodeName().equals("Permission")) {
                    grant.permission = Permission.valueOf(child.getChildNodes().item(0).getNodeValue());
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
