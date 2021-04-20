package com.thorinhood.utils;

import com.thorinhood.data.S3User;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;

import java.net.URI;
import java.util.List;
import java.util.Map;

public class ParsedRequest {

    private byte[] bytes;
    private String bucket;
    private String key;
    private String signature;
    private Credential credential;
    private Integer decodedContentLength;
    private PayloadSignType payloadSignType;
    private HttpHeaders headers;
    private Map<String, List<String>> queryParams;
    private HttpMethod method;
    private Map<String, String> metadata;
    private S3User s3User;
    private String uri;

    public static Builder builder() {
        return new Builder();
    }

    private ParsedRequest() {
    }

    public boolean isPathToObject() {
        return !key.equals("");
    }

    public byte[] getBytes() {
        return bytes;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public String getSignature() {
        return signature;
    }

    public Credential getCredential() {
        return credential;
    }

    public Integer getDecodedContentLength() {
        return decodedContentLength;
    }

    public PayloadSignType getPayloadSignType() {
        return payloadSignType;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public boolean containsHeader(String header) {
        return headers.contains(header);
    }

    public String getHeader(String header) {
        return headers.get(header);
    }

    public HttpHeaders getHeaders() {
        return headers;
    }

    public HttpMethod getMethod() {
        return method;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public Map<String, List<String>> getQueryParams() {
        return queryParams;
    }

    public S3User getS3User() {
        return s3User;
    }

    public String getRawUri() {
        return uri;
    }

    public static class Builder {
        private final ParsedRequest parsedRequest;

        public Builder() {
            parsedRequest = new ParsedRequest();
        }

        public Builder setBytes(byte[] bytes) {
            parsedRequest.bytes = bytes;
            return this;
        }

        public Builder setBucket(String bucket) {
            parsedRequest.bucket = bucket;
            return this;
        }

        public Builder setKey(String key) {
            parsedRequest.key = key;
            return this;
        }

        public Builder setSignature(String signature) {
            parsedRequest.signature = signature;
            return this;
        }

        public Builder setCredential(Credential credential) {
            parsedRequest.credential = credential;
            return this;
        }

        public Builder setDecodedContentLength(Integer decodedContentLength) {
            parsedRequest.decodedContentLength = decodedContentLength;
            return this;
        }

        public Builder setPayloadSignType(PayloadSignType payloadSignType) {
            parsedRequest.payloadSignType = payloadSignType;
            return this;
        }

        public Builder setHeaders(HttpHeaders headers) {
            parsedRequest.headers = headers;
            return this;
        }

        public Builder setQueryParams(Map<String, List<String>> queryParams) {
            parsedRequest.queryParams = queryParams;
            return this;
        }

        public Builder setMethod(HttpMethod method) {
            parsedRequest.method = method;
            return this;
        }

        public Builder setMetadata(Map<String, String> metadata) {
            parsedRequest.metadata = metadata;
            return this;
        }

        public Builder setS3User(S3User s3User) {
            parsedRequest.s3User = s3User;
            return this;
        }

        public Builder setRawUri(String uri) {
            parsedRequest.uri = uri;
            return this;
        }

        public ParsedRequest build() {
            return parsedRequest;
        }
    }
}
