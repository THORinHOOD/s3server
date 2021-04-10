package com.thorinhood.processors.selectors;

import com.thorinhood.data.S3ResponseErrorCodes;
import com.thorinhood.exceptions.S3Exception;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Date;
import java.util.Optional;

public class IfModifiedSince implements Selector<Date> {

    @Override
    public void check(Date actual, Date expected) throws S3Exception {
        throw S3Exception.build("Not found IfModifiedSince")
                    .setStatus(HttpResponseStatus.NOT_MODIFIED)
                    .setCode(S3ResponseErrorCodes.INVALID_REQUEST)
                    .setMessage("null")
                    .setResource("1")
                    .setRequestId("1");

    }

}
