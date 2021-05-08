package com.thorinhood.drivers;

import com.thorinhood.exceptions.S3Exception;

import java.io.IOException;

public interface S3Supplier<T> {
    T get() throws S3Exception, IOException;
}
