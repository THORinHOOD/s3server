package com.thorinhood.exceptions;

import com.thorinhood.data.S3ResponseErrorCodes;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import com.thorinhood.utils.XmlUtil;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

public class S3Exception extends RuntimeException implements HasStatus, HasCode, HasMessage, HasResource, HasRequestId {

    private HttpResponseStatus status;
    private String code;
    private String message;
    private String resource;
    private String requestId;

    public static HasMessage INTERNAL_ERROR(String message) {
        return build(message)
                .setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                .setCode(S3ResponseErrorCodes.INTERNAL_ERROR);
    }

    public static HasStatus build(String internalMessage) {
        return new S3Exception(internalMessage);
    }

    private S3Exception(String message) {
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

    @Override
    public HasCode setStatus(HttpResponseStatus status) {
        this.status = status;
        return this;
    }

    public HttpResponseStatus getStatus() {
        return status;
    }

    @Override
    public HasMessage setCode(String code) {
        this.code = code;
        return this;
    }

    public String getCode() {
        return code;
    }

    public String getS3Message() {
        return message;
    }

    @Override
    public HasRequestId setResource(String resource) {
        this.resource = resource;
        return this;
    }

    public String getResource() {
        return resource;
    }

    @Override
    public S3Exception setRequestId(String requestId) {
        this.requestId = requestId;
        return this;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getXml() {
        return buildXml(code, message, resource, requestId);
    }

    @Override
    public HasResource setMessage(String message) {
        this.message = message;
        return this;
    }
}
