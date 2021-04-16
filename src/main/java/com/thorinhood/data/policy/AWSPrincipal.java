package com.thorinhood.data.policy;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class AWSPrincipal {

    private List<String> links;

    public AWSPrincipal(@JsonProperty(value = "AWS", required = true) Object links) {
        if (links instanceof ArrayList) {
            this.links = (List<String>) links;
        } else {
            this.links = new ArrayList<>();
            this.links.add((String) links);
        }
    }

    public List<String> getAWS() {
        return links;
    }

}
