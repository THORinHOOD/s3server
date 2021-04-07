package com.thorinhood.chunks;

public class ChunkInfo {

    private final int chunkSize;
    private final String signature;

    public ChunkInfo(int chunkSize, String signature) {
        this.chunkSize = chunkSize;
        this.signature = signature;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public String getSignature() {
        return signature;
    }
}
