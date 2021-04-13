package com.thorinhood.utils;

import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.utils.XmlUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.util.Map;

public interface XmlObject {

    default String buildXml() throws S3Exception {
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

    default Element createElement(Document doc, String key, Node... values) {
        Element element = doc.createElement(key);
        doc.createTextNode("");
        for (Node child : values) {
            element.appendChild(child);
        }
        return element;
    }

    default Element appendAttributes(Element element, Map<String, String> attributes) {
        attributes.forEach(element::setAttribute);
        return element;
    }

    Element buildXmlRootNode(Document doc);
}
