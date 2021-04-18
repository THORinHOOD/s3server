package com.thorinhood.data.policy;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class BucketPolicy {

    @JsonProperty(value = "Version")
    private String version;
    @JsonProperty(value = "Id")
    private String id;
    @JsonProperty(value = "Statement", required = true)
    private List<Statement> statements;

    public BucketPolicy() {
    }

    public BucketPolicy(String version, String id, List<Statement> statements) {
        this.version = version;
        this.id = id;
        this.statements = statements;
    }

    public String getVersion() {
        return version;
    }

    public String getId() {
        return id;
    }

    public List<Statement> getStatements() {
        return statements;
    }
}
