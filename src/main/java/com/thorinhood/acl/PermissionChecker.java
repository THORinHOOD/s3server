package com.thorinhood.acl;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PermissionChecker {

    protected static boolean checkPermission(AccessControlPolicy acl, Permission... permissions) {
        Set<Permission> allPermissions = acl.getAccessControlList().stream()
                .map(Grant::getPermission)
                .collect(Collectors.toSet());
        return Stream.of(permissions)
                .anyMatch(allPermissions::contains);
    }

}
