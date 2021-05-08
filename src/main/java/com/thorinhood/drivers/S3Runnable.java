package com.thorinhood.drivers;

import com.thorinhood.exceptions.S3Exception;

import java.io.IOException;

public interface S3Runnable {
    void run() throws S3Exception, IOException;
}