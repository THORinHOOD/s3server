package com.thorinhood.processors.selectors;

import com.thorinhood.data.requests.S3ResponseErrorCodes;
import com.thorinhood.exceptions.S3Exception;
import io.netty.handler.codec.http.HttpResponseStatus;

public class IfMatch implements Selector<String> {
    @Override
    public void check(String actual, String expected) throws S3Exception {
        if (!actual.equals(expected)) {
            throw S3Exception.builder("IfMatch failed")
                    .setStatus(HttpResponseStatus.PRECONDITION_FAILED)
                    .setCode(S3ResponseErrorCodes.PRECONDITION_FAILED)
                    .setMessage("At least one of the pre-conditions you specified did not hold")
                    .build();
        }
    }
}
