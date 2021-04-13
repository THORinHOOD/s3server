package com.thorinhood.exceptions;

import com.thorinhood.data.S3ResponseErrorCodes;
import com.thorinhood.utils.XmlObject;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class S3Exception extends RuntimeException implements HasStatus, HasCode, HasMessage, HasResource, HasRequestId,
        XmlObject {

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

    @Override
    public HasResource setMessage(String message) {
        this.message = message;
        return this;
    }

    @Override
    public Element buildXmlRootNode(Document doc) {
        return createElement(doc, "Error",
                createElement(doc, "Code", doc.createTextNode(code)),
                createElement(doc, "Message", doc.createTextNode(message)),
                createElement(doc, "Resource", doc.createTextNode(resource)),
                createElement(doc, "RequestId", doc.createTextNode(requestId)));
    }

}
