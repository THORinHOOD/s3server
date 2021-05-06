package com.thorinhood.data.multipart;

import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

public class CompleteMultipartUpload {

    private List<Part> parts;

    public static CompleteMultipartUpload buildFromNode(Node node) {
        List<Part> parts = new ArrayList<>();
        for (int i = 0; i < node.getChildNodes().getLength(); i++) {
            Node child = node.getChildNodes().item(i);
            if (child.getNodeName().equals("Part")) {
                parts.add(Part.buildFromNode(child));
            }
        }
        return new CompleteMultipartUpload(parts);
    }

    private CompleteMultipartUpload(List<Part> parts) {
        this.parts = parts;
    }

    public List<Part> getParts() {
        return parts;
    }

}
