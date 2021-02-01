package com.thorinhood.exceptions;

import io.netty.handler.codec.http.HttpResponseStatus;

public interface HasStatus {
    HasCode setStatus(HttpResponseStatus status);
    HttpResponseStatus getStatus();
}
