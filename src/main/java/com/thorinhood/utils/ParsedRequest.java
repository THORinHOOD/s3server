package com.thorinhood.utils;

public class ParsedRequest {

    private byte[] bytes;
    private final String bucket;
    private final String key;
    private final String signature;
    private final Credential credential;
    private final Integer decodedContentLength;
    private final PayloadSignType payloadSignType;
    private final String ifMatchHeader;
    private final String ifModifiedHeader;
    private final String ifNoneMatchHeader;
    private final String ifUnmodifiedSince;

    public ParsedRequest(byte[] bytes, String bucket, String key, String signature, Credential credential,
                         Integer decodedContentLength, PayloadSignType payloadSignType, String ifMatchHeader,
                         String ifModifiedHeader, String ifNoneMatchHeader, String ifUnmodifiedSince) {
        this.bytes = bytes;
        this.bucket = bucket;
        this.key = key;
        this.signature = signature;
        this.credential = credential;
        this.decodedContentLength = decodedContentLength;
        this.payloadSignType = payloadSignType;
        this.ifMatchHeader = ifMatchHeader;
        this.ifModifiedHeader = ifModifiedHeader;
        this.ifNoneMatchHeader = ifNoneMatchHeader;
        this.ifUnmodifiedSince = ifUnmodifiedSince;
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

    public String getIfMatchHeader() {
        return ifMatchHeader;
    }

    public String getIfModifiedHeader() {
        return ifModifiedHeader;
    }

    public String getIfNoneMatchHeader() {
        return ifNoneMatchHeader;
    }

    public String getIfUnmodifiedSince() {
        return ifUnmodifiedSince;
    }
}
