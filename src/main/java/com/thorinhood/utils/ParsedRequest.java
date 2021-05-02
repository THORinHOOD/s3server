package com.thorinhood.utils;

import com.thorinhood.data.S3ResponseErrorCodes;
import com.thorinhood.data.S3User;
import com.thorinhood.exceptions.S3Exception;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

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
    private Set<String> signedHeaders;

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

    public Set<String> getSignedHeaders() {
        return signedHeaders;
    }

    public <T> T getQueryParam(String key, T defaultValue, Function<String, T> converter) throws S3Exception {
        if (!queryParams.containsKey(key)) {
            return defaultValue;
        }
        List<String> values = queryParams.get(key);
        if (values.size() != 1) {
            return defaultValue;
        }
        try {
            return converter.apply(values.get(0));
        } catch (Exception exception) {
            throw S3Exception.build("Can't parse query parameter")
                    .setStatus(HttpResponseStatus.BAD_REQUEST)
                    .setCode(S3ResponseErrorCodes.INVALID_ARGUMENT)
                    .setMessage("Can't parse query parameter : " + key)
                    .setResource("1")
                    .setRequestId("1"); // TODO
        }
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

        public Builder setSignedHeaders(Set<String> signedHeaders) {
            parsedRequest.signedHeaders = signedHeaders;
            return this;
        }

        public ParsedRequest build() {
            return parsedRequest;
        }
    }
}
