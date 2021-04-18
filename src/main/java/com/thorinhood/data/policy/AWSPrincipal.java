package com.thorinhood.data.policy;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class AWSPrincipal {

    @JsonProperty(value = "AWS", required = true)
    @JsonFormat(with = { JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY,
                         JsonFormat.Feature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED })
    private List<String> aws;

    public AWSPrincipal() {}

    public AWSPrincipal(List<String> aws) {
        this.aws = aws;
    }

    public List<String> getAWS() {
        return aws;
    }

}
