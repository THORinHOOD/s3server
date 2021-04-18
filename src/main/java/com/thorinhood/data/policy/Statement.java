package com.thorinhood.data.policy;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Statement {

    public enum EffectType {
        Allow, Deny;
    }

    @JsonProperty(value = "Sid")
    private String sid;
    @JsonProperty(value = "Effect", required = true)
    private EffectType effect;
    @JsonProperty(value = "Principal", required = true)
    private AWSPrincipal principle;
    @JsonProperty(value = "Action", required = true)
    @JsonFormat(with = { JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY,
                         JsonFormat.Feature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED })
    private List<String> action;
    @JsonProperty(value = "Resource", required = true)
    @JsonFormat(with = { JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY,
                         JsonFormat.Feature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED })
    private List<String> resource;

    public Statement() {
    }

    public Statement(String sid, EffectType effect, AWSPrincipal principle, List<String> action, List<String> resource) {
        this.sid = sid;
        this.effect = effect;
        this.principle = principle;
        this.action = action;
        this.resource = resource;
    }

    public String getSid() {
        return sid;
    }

    public EffectType getEffect() {
        return effect;
    }

    public AWSPrincipal getPrinciple() {
        return principle;
    }

    public List<String> getAction() {
        return action;
    }

    public List<String> getResource() {
        return resource;
    }
}
