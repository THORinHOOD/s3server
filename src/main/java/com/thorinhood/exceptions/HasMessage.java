package com.thorinhood.exceptions;

public interface HasMessage {
    HasResource setMessage(String message);
    String getMessage();
}
