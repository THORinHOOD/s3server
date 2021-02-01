package com.thorinhood.exceptions;

public interface HasRequestId {
    S3Exception setRequestId(String requestId);
    String getRequestId();
}
