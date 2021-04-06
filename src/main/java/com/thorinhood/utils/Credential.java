package com.thorinhood.utils;

import io.netty.handler.codec.http.FullHttpRequest;

import java.util.Map;

public class Credential {

    public static final String ACCESS_KEY = "accessKey";
    public static final String DATE = "date";
    public static final String REGION_NAME = "regionName";
    public static final String SERVICE_NAME = "serviceName";
    public static final String REQUEST_TYPE = "requestType";

    private final Map<String, String> keyValue;
    private final String credential;
    private final String credentialWithoutAccessKey;

    public static Credential parse(FullHttpRequest request) {
        String credential = getCredential(request);
        String credentialWithoutAccessKey = getCredentialWithoutAccessKey(credential);
        Map<String, String> keyValue = extractCredentials(credential);
        return new Credential(keyValue, credential, credentialWithoutAccessKey);
    }

    private Credential(Map<String, String> keyValue, String credential, String credentialWithoutAccessKey) {
        this.keyValue = keyValue;
        this.credential = credential;
        this.credentialWithoutAccessKey = credentialWithoutAccessKey;
    }

    private static Map<String, String> extractCredentials(String credential) {
        int slash = credential.indexOf("/");
        int slash2 = credential.indexOf("/", slash + 1);
        int slash3 = credential.indexOf("/", slash2 + 1);
        int slash4 = credential.indexOf("/", slash3 + 1);
        String accessKey = credential.substring(0, slash);
        String date = credential.substring(slash + 1, slash2);
        String regionName = credential.substring(slash2 + 1, slash3);
        String serviceName = credential.substring(slash3 + 1, slash4);
        String requestType = credential.substring(slash4 + 1);
        return Map.of(
                ACCESS_KEY, accessKey,
                DATE, date,
                REGION_NAME, regionName,
                SERVICE_NAME, serviceName,
                REQUEST_TYPE, requestType
        );
    }

    private static String getCredentialWithoutAccessKey(String credential) {
        return credential.substring(credential.indexOf("/") + 1);
    }

    private static String getCredential(FullHttpRequest request) {
        String authorization = request.headers().get("Authorization");
        String credential = authorization.substring(authorization.indexOf("Credential=") + "Credential=".length());
        credential = credential.substring(0, credential.indexOf(",")).trim();
        return credential;
    }

    public String getValue(String key) {
        return keyValue.get(key);
    }

    public String getCredential() {
        return credential;
    }

    public String getCredentialWithoutAccessKey() {
        return credentialWithoutAccessKey;
    }
}
