package com.thorinhood.exceptions;

public interface HasResource {
    HasRequestId setResource(String resource);
    String getResource();
}
