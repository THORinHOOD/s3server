package exceptions;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import utils.XmlUtil;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

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
            return XmlUtil.xmlDocumentToString(doc);
        } catch (ParserConfigurationException e) {
            //TODO
            return "";
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
