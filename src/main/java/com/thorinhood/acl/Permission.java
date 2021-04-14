package com.thorinhood.acl;

import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public enum Permission {
    READ (
            Set.of("s3:ListBucket", "s3:ListBucketVersions", "s3:ListBucketMultipartUploads"),
            Set.of("s3:GetObject", "s3:GetObjectVersion", "s3:GetObjectTorrent")
    ),
    WRITE (
            Set.of("s3:PutObject", "s3:DeleteObject"), // TODO s3:DeleteObjectVersion
            Set.of()
    ),
    READ_ACP (
            Set.of("s3:GetBucketAcl"),
            Set.of("s3:GetObjectAcl", "s3:GetObjectVersionAcl")
    ),
    WRITE_ACP (
            Set.of("s3:PutBucketAcl"),
            Set.of("s3:PutObjectAcl", "s3:PutObjectVersionAcl")
    ),
    FULL_CONTROL (
            ImmutableSet.of(
                    READ.getMethodsBucket(),
                    WRITE.getMethodsBucket(),
                    READ_ACP.getMethodsBucket(),
                    WRITE_ACP.getMethodsBucket()
            ).stream().flatMap(Collection::stream).collect(Collectors.toSet()),
            ImmutableSet.of(
                    READ.getMethodsObject(),
                    WRITE.getMethodsObject(),
                    READ_ACP.getMethodsObject(),
                    WRITE_ACP.getMethodsObject()
            ).stream().flatMap(Collection::stream).collect(Collectors.toSet())
    );

    private final Set<String> methodsBucket;
    private final Set<String> methodsObject;

    Permission(Set<String> methodsBucket, Set<String> methodsObject) {
        this.methodsBucket = methodsBucket;
        this.methodsObject = methodsObject;
    }

    public Set<String> getMethodsBucket() {
        return methodsBucket;
    }

    public Set<String> getMethodsObject() {
        return methodsObject;
    }
}
