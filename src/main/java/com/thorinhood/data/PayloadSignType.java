package com.thorinhood.data;

public enum PayloadSignType {
    CHUNKED("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
    UNSIGNED_PAYLOAD("UNSIGNED_PAYLOAD"),
    SINGLE_CHUNK(null);

    private String value;

    PayloadSignType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}