package com.thorinhood.acl;

import com.thorinhood.BaseTest;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.util.List;

public class AclTest extends BaseTest {
    public AclTest() {
        super("testS3Java", 9999);
    }

    @Test
    public void putAndGetBucketAcl() throws IOException {
        S3Client s3Client = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        S3Client s3Client2 = getS3Client(true, ROOT_USER_2.getAccessKey(), ROOT_USER_2.getSecretKey());
        createBucketRaw(s3Client, "bucket");
        String content = createContent(500);
        assertException(HttpResponseStatus.FORBIDDEN.code(), S3ResponseErrorCodes.ACCESS_DENIED, () ->
                putObjectRaw(s3Client2, "bucket", "file.txt", content, null));

        AccessControlPolicy acl = AccessControlPolicy.builder()
                .owner(Owner.builder()
                        .displayName(ROOT_USER.getAccountName())
                        .id(ROOT_USER.getCanonicalUserId())
                        .build())
                .grants(Grant.builder()
                                .permission("WRITE")
                                .grantee(Grantee.builder()
                                        .displayName(ROOT_USER_2.getAccountName())
                                        .id(ROOT_USER_2.getCanonicalUserId())
                                        .type(Type.CANONICAL_USER)
                                        .build())
                                .build(),
                        Grant.builder()
                                .permission("FULL_CONTROL")
                                .grantee(Grantee.builder()
                                        .displayName(ROOT_USER.getAccountName())
                                        .id(ROOT_USER.getCanonicalUserId())
                                        .type(Type.CANONICAL_USER)
                                        .build())
                                .build())
                .build();
        s3Client.putBucketAcl(PutBucketAclRequest.builder()
                .bucket("bucket")
                .accessControlPolicy(acl)
                .build());
        putObjectRaw(s3Client2, "bucket", "file.txt", content, null);
        checkObject("bucket", null, "file.txt", content, null);

        GetBucketAclResponse response = s3Client.getBucketAcl(GetBucketAclRequest.builder()
                .bucket("bucket")
                .build());
        assertAcl(acl.grants(), acl.owner(), response.grants(), response.owner());
    }

    @Test
    public void putAndGetObjectAcl() {
        S3Client s3Client = getS3Client(true, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());
        S3Client s3Client2 = getS3Client(true, ROOT_USER_2.getAccessKey(), ROOT_USER_2.getSecretKey());
        createBucketRaw(s3Client, "bucket");
        String content = createContent(500);
        putObjectRaw(s3Client, "bucket", "file.txt", content, null);
        assertException(HttpResponseStatus.FORBIDDEN.code(), S3ResponseErrorCodes.ACCESS_DENIED, () ->
                getObject(s3Client2, "bucket", "file.txt", content, null));
        AccessControlPolicy acl = AccessControlPolicy.builder()
                .owner(Owner.builder()
                        .displayName(ROOT_USER.getAccountName())
                        .id(ROOT_USER.getCanonicalUserId())
                        .build())
                .grants(Grant.builder()
                                .permission("READ")
                                .grantee(Grantee.builder()
                                        .displayName(ROOT_USER_2.getAccountName())
                                        .id(ROOT_USER_2.getCanonicalUserId())
                                        .type(Type.CANONICAL_USER)
                                        .build())
                                .build(),
                        Grant.builder()
                                .permission("FULL_CONTROL")
                                .grantee(Grantee.builder()
                                        .displayName(ROOT_USER.getAccountName())
                                        .id(ROOT_USER.getCanonicalUserId())
                                        .type(Type.CANONICAL_USER)
                                        .build())
                                .build())
                .build();
        s3Client.putObjectAcl(PutObjectAclRequest.builder()
                .bucket("bucket")
                .key("file.txt")
                .accessControlPolicy(acl)
                .build());
        getObject(s3Client2, "bucket", "file.txt", content, null);
        GetObjectAclResponse response = s3Client.getObjectAcl(GetObjectAclRequest.builder()
                .bucket("bucket")
                .key("file.txt")
                .build());
        assertAcl(acl.grants(), acl.owner(), response.grants(), response.owner());
    }

    private void assertAcl(List<Grant> expectedGrants, Owner expectedOwner, List<Grant> actualGrants,
                           Owner actualOwner) {
        Assertions.assertEquals(expectedOwner.id(), actualOwner.id());
        Assertions.assertEquals(expectedOwner.displayName(), actualOwner.displayName());
        Assertions.assertEquals(expectedGrants.size(), actualGrants.size());
        for (Grant expectedGrant : expectedGrants) {
            Assertions.assertTrue(actualGrants.stream()
                    .anyMatch(actualGrant -> equalsGrants(expectedGrant, actualGrant)));
        }
    }

    private boolean equalsGrants(Grant expectedGrant, Grant actualGrant) {
        return expectedGrant.grantee().id().equals(actualGrant.grantee().id()) &&
                expectedGrant.grantee().displayName().equals(actualGrant.grantee().displayName()) &&
                expectedGrant.permission().equals(actualGrant.permission());
    }
}
