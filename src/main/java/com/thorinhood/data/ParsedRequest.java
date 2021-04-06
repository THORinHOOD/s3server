package com.thorinhood.data;

public class ParsedRequest {

    private final byte[] bytes;
    private final String bucket;
    private final String key;
    private final String seedSignature;

    public ParsedRequest(byte[] bytes, String bucket, String key, String seedSignature) {
        this.bytes = bytes;
        this.bucket = bucket;
        this.key = key;
        this.seedSignature = seedSignature;
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
}
