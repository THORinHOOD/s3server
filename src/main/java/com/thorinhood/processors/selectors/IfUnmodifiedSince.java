package com.thorinhood.processors.selectors;

import com.thorinhood.data.requests.S3ResponseErrorCodes;
import com.thorinhood.exceptions.S3Exception;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Date;

public class IfUnmodifiedSince implements Selector<Date> {
    @Override
    public void check(Date actual, Date expected) throws S3Exception {
        if (actual.after(expected)) {
            throw S3Exception.builder("IfUnmodifiedSince failed")
                    .setStatus(HttpResponseStatus.NOT_MODIFIED)
                    .setCode(S3ResponseErrorCodes.INVALID_REQUEST)
                    .setMessage("At least one of the pre-conditions you specified did not hold")
                    .build();
        }
    }
}
