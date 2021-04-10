package com.thorinhood.utils;

import io.netty.handler.codec.http.HttpHeaders;

public class ParsedRequest {

    private byte[] bytes;
    private final String bucket;
    private final String key;
    private final String signature;
    private final Credential credential;
    private final Integer decodedContentLength;
    private final PayloadSignType payloadSignType;
    private final HttpHeaders headers;

    public ParsedRequest(byte[] bytes, String bucket, String key, String signature, Credential credential,
                         Integer decodedContentLength, PayloadSignType payloadSignType, HttpHeaders headers) {
        this.bytes = bytes;
        this.bucket = bucket;
        this.key = key;
        this.signature = signature;
        this.credential = credential;
        this.decodedContentLength = decodedContentLength;
        this.payloadSignType = payloadSignType;
        this.headers = headers;
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
}
