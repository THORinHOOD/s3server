package com.thorinhood.processors.selectors;

import com.thorinhood.data.S3ResponseErrorCodes;
import com.thorinhood.exceptions.S3Exception;
import io.netty.handler.codec.http.HttpResponseStatus;

public class IfNoneMatch implements Selector<String> {

    @Override
    public void check(String actual, String expected) {
        throw S3Exception.build("Not found IfNoneMatch")
                .setStatus(HttpResponseStatus.NOT_MODIFIED)
                .setCode(S3ResponseErrorCodes.INVALID_REQUEST)
                .setMessage("null")
                .setResource("1")
                .setRequestId("1");
    }
}
