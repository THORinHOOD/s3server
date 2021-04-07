package com.thorinhood.data;

import com.thorinhood.utils.Credential;

public class ParsedRequest {

    private byte[] bytes;
    private final String bucket;
    private final String key;
    private final String signature;
    private final Credential credential;
    private final Integer decodedContentLength;
    private final PayloadSignType payloadSignType;

    public ParsedRequest(byte[] bytes, String bucket, String key, String signature, Credential credential,
                         Integer decodedContentLength, PayloadSignType payloadSignType) {
        this.bytes = bytes;
        this.bucket = bucket;
        this.key = key;
        this.signature = signature;
        this.credential = credential;
        this.decodedContentLength = decodedContentLength;
        this.payloadSignType = payloadSignType;
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

}
