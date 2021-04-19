package com.thorinhood.utils;

import com.thorinhood.exceptions.S3Exception;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.util.Map;

public interface XmlObject {

    default String buildXmlText() throws S3Exception {
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            Document doc = docBuilder.newDocument();
            doc.setXmlStandalone(true);
            doc.appendChild(buildXmlRootNode(doc));
            return XmlUtil.xmlDocumentToString(doc);
        } catch (ParserConfigurationException e) {
            throw S3Exception.INTERNAL_ERROR(e.getMessage())
                    .setMessage(e.getMessage())
                    .setResource("1")
                    .setRequestId("1");
        }
    }

    default Element createTextElement(Document doc, String key, String value) {
        if (value == null) {
            return null;
        }
        return createElement(doc, key, doc.createTextNode(value));
    }

    default Element createElement(Document doc, String key, Node... values) {
        Element element = doc.createElement(key);
        for (Node child : values) {
            if (child != null) {
                element.appendChild(child);
            }
        }
        return element;
    }

    default Element appendAttributes(Element element, Map<String, String> attributes) {
        attributes.forEach((key, value) -> {
            if (value != null) {
                element.setAttribute(key, value);
            }
        });
        return element;
    }

    Element buildXmlRootNode(Document doc);
}
