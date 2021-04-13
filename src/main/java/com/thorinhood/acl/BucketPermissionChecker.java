package com.thorinhood.acl;

public class BucketPermissionChecker extends PermissionChecker {

    public static boolean canListObjects(AccessControlPolicy acl) {
        return checkPermission(acl,
                Permission.READ,
                Permission.FULL_CONTROL);
    }

    public static boolean canCreateOverriteDeleteObject(AccessControlPolicy acl) {
        return checkPermission(acl,
                Permission.WRITE,
                Permission.FULL_CONTROL);
    }

    public static boolean canReadAcl(AccessControlPolicy acl) {
        return checkPermission(acl,
                Permission.READ_ACP,
                Permission.FULL_CONTROL);
    }

    public static boolean canWriteAcl(AccessControlPolicy acl) {
        return checkPermission(acl,
                Permission.WRITE_ACP,
                Permission.FULL_CONTROL);
    }

}
