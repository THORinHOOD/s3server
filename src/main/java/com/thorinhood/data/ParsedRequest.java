package com.thorinhood.data;

import com.thorinhood.utils.Credential;

public class ParsedRequest {

    private byte[] bytes;
    private final String bucket;
    private final String key;
    private final String seedSignature;
    private final Credential credential;
    private final int decodedContentLength;
    private final PayloadSignType payloadSignType;

    public ParsedRequest(byte[] bytes, String bucket, String key, String seedSignature, Credential credential,
                         int decodedContentLength, PayloadSignType payloadSignType) {
        this.bytes = bytes;
        this.bucket = bucket;
        this.key = key;
        this.seedSignature = seedSignature;
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

    public String getSeedSignature() {
        return seedSignature;
    }

    public Credential getCredential() {
        return credential;
    }

    public int getDecodedContentLength() {
        return decodedContentLength;
    }

    public PayloadSignType getPayloadSignType() {
        return payloadSignType;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

}
