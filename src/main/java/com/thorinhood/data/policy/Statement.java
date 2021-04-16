package com.thorinhood.data.policy;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Statement {

    public enum EffectType {
        Allow, Deny;
    }

    private String sid;
    private EffectType effect;
    private AWSPrincipal principle;
    private List<String> action;
    private List<String> resource;

    public Statement(@JsonProperty(value = "Sid") String sid,
                     @JsonProperty(value = "Effect", required = true) EffectType effect,
                     @JsonProperty(value = "Principal", required = true) AWSPrincipal principle,
                     @JsonProperty(value = "Action", required = true) List<String> action,
                     @JsonProperty(value = "Resource", required = true) List<String> resource) {
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
