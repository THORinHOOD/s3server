package com.thorinhood.data.policy;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class BucketPolicy {

    private String version;
    private String id;
    private List<Statement> statements;

    public BucketPolicy(@JsonProperty(value = "Version") String version,
                        @JsonProperty(value = "Id") String id,
                        @JsonProperty(value = "Statement", required = true) List<Statement> statements) {
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
