package com.thorinhood.acl;

public class ObjectPermissionChecker extends PermissionChecker {

    public static boolean canReadDataAndMetadata(AccessControlPolicy acl) {
        return checkPermission(acl,
                Permission.READ,
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
