package com.thorinhood.exceptions;

import com.thorinhood.data.S3FileBucketPath;
import com.thorinhood.utils.XmlObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.File;

public class S3ExceptionFull extends S3Exception implements XmlObject {

    private final String requestId;
    private final S3FileBucketPath resource;

    public static S3ExceptionFull build(S3Exception s3Exception, S3FileBucketPath resource, String requestId) {
        return new S3ExceptionFull(s3Exception, resource, requestId);
    }

    private S3ExceptionFull(S3Exception s3Exception, S3FileBucketPath resource, String requestId) {
        super(s3Exception.getMessage());
        this.resource = resource;
        this.requestId = requestId;
        this.code = s3Exception.getCode();
        this.status = s3Exception.getStatus();
        this.message = s3Exception.getS3Message();
    }

    @Override
    public Element buildXmlRootNode(Document doc) {
        return createElement(doc, "Error",
                createTextElement(doc, "Code", code),
                createTextElement(doc, "Message", message),
                createTextElement(doc, "Resource", resource == null ? "" : File.separatorChar +
                        resource.getKeyWithBucket()),
                createTextElement(doc, "RequestId", requestId));
    }

    public String getRequestId() {
        return requestId;
    }

    public S3FileBucketPath getResource() {
        return resource;
    }
}
