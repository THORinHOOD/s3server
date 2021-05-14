package com.thorinhood.exceptions;

import com.thorinhood.data.requests.S3ResponseErrorCodes;
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

    public static HasResource ACCESS_DENIED() {
        return build("Access denied")
                .setStatus(HttpResponseStatus.FORBIDDEN)
                .setCode(S3ResponseErrorCodes.ACCESS_DENIED)
                .setMessage("Access denied");
    }

    public static HasMessage INTERNAL_ERROR(String message) {
        return build(message)
                .setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                .setCode(S3ResponseErrorCodes.INTERNAL_ERROR);
    }

    public static HasResource INTERNAL_ERROR(Exception exception) {
        return build(exception.getMessage() != null ? exception.getMessage() : exception.toString())
                .setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                .setCode(S3ResponseErrorCodes.INTERNAL_ERROR)
                .setMessage(exception.getMessage() != null ? exception.getMessage() : exception.toString());
    }

    public static S3Exception NO_SUCH_UPLOAD(String uploadId) {
        return S3Exception.build("No such upload : " + uploadId)
                .setStatus(HttpResponseStatus.NOT_FOUND)
                .setCode(S3ResponseErrorCodes.NO_SUCH_UPLOAD)
                .setMessage("The specified multipart upload does not exist. The upload ID might be invalid, " +
                        "or the multipart upload might have been aborted or completed.")
                .setResource("1")
                .setRequestId("1");
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
