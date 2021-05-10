package com.thorinhood.processors.selectors;

import com.thorinhood.data.requests.S3ResponseErrorCodes;
import com.thorinhood.exceptions.S3Exception;
import io.netty.handler.codec.http.HttpResponseStatus;

public class IfNoneMatch implements Selector<String> {
    @Override
    public void check(String actual, String expected) throws S3Exception {
        if (actual.equals(expected)) {
            throw S3Exception.build("IfNondeMatch failed")
                    .setStatus(HttpResponseStatus.NOT_MODIFIED)
                    .setCode(S3ResponseErrorCodes.INVALID_REQUEST)
                    .setMessage("At least one of the pre-conditions you specified did not hold")
                    .setResource("1")
                    .setRequestId("1");
        }
    }
}
