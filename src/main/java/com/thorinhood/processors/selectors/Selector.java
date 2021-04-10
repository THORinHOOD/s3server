package com.thorinhood.processors.selectors;

import software.amazon.awssdk.services.s3.model.S3Exception;

public interface Selector<T> {

    void check(T actual, T expected) throws S3Exception;

}
