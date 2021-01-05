package exceptions;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;

public class S3Exception extends RuntimeException {

    private HttpResponseStatus status;
    private String code;
    private String message;
    private String resource;
    private String requestId;

    public S3Exception(String message) {
        super(message);
    }

    private String buildXml(String code, String message, String resource, String requestId) {
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            Document doc = docBuilder.newDocument();
            doc.setXmlStandalone(true);
            Element error = doc.createElement("Error");
            error.appendChild(createElement(doc, "Code", code));
            error.appendChild(createElement(doc, "Message", message));
            error.appendChild(createElement(doc, "Resource", resource));
            error.appendChild(createElement(doc, "RequestId", requestId));
            doc.appendChild(error);
            return xmlDocumentToString(doc);
        } catch (ParserConfigurationException e) {
            //TODO
            return "";
        }
    }

    private String xmlDocumentToString(Document doc) {
        try {
            StringWriter sw = new StringWriter();
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.METHOD, "xml");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");

            transformer.transform(new DOMSource(doc), new StreamResult(sw));
            return sw.toString();
        } catch (Exception ex) {
            throw new RuntimeException("Error converting to String", ex);
        }
    }

    private Element createElement(Document doc, String key, String value) {
        Element element = doc.createElement(key);
        element.appendChild(doc.createTextNode(value));
        return element;
    }

    public S3Exception setStatus(HttpResponseStatus status) {
        this.status = status;
        return this;
    }

    public S3Exception setCode(String code) {
        this.code = code;
        return this;
    }

    public S3Exception setMessage(String message) {
        this.message = message;
        return this;
    }

    public S3Exception setResource(String resource) {
        this.resource = resource;
        return this;
    }

    public S3Exception setRequestId(String requestId) {
        this.requestId = requestId;
        return this;
    }

    public HttpResponseStatus getStatus() {
        return status;
    }

    public String getCode() {
        return code;
    }

    public String getS3Message() {
        return message;
    }

    public String getResource() {
        return resource;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getXml() {
        return buildXml(code, message, resource, requestId);
    }
}
