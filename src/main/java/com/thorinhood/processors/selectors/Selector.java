package com.thorinhood.processors.selectors;

import com.thorinhood.exceptions.S3Exception;

public interface Selector<T> {

    void check(T actual, T expected) throws S3Exception;

}
