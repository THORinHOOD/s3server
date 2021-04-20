package com.thorinhood.drivers.main;

import com.thorinhood.data.S3Content;
import com.thorinhood.data.S3ResponseErrorCodes;
import com.thorinhood.data.S3User;
import com.thorinhood.data.acl.*;
import com.thorinhood.data.policy.BucketPolicy;
import com.thorinhood.data.policy.Statement;
import com.thorinhood.data.s3object.HasMetaData;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.drivers.acl.AclDriver;
import com.thorinhood.drivers.entity.EntityDriver;
import com.thorinhood.drivers.metadata.MetadataDriver;
import com.thorinhood.drivers.principal.PolicyDriver;
import com.thorinhood.exceptions.S3Exception;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class S3DriverImpl implements S3Driver {

    private final MetadataDriver metadataDriver;
    private final AclDriver aclDriver;
    private final PolicyDriver policyDriver;
    private final EntityDriver entityDriver;

    private static final Logger log = LogManager.getLogger(S3DriverImpl.class);

    public S3DriverImpl(MetadataDriver metadataDriver, AclDriver aclDriver, PolicyDriver policyDriver,
                        EntityDriver entityDriver) {
        this.metadataDriver = metadataDriver;
        this.aclDriver = aclDriver;
        this.policyDriver = policyDriver;
        this.entityDriver = entityDriver;
    }

    @Override
    public boolean checkBucketPolicy(String bucket, String key, String methodName, S3User s3User) throws S3Exception {
        Optional<BucketPolicy> bucketPolicy = getBucketPolicy(bucket);
        if (bucketPolicy.isEmpty()) {
            return s3User.isRootUser();
        }
        boolean result = s3User.isRootUser();
        for (Statement statement : bucketPolicy.get().getStatements()) {
            if (checkPrincipal(statement.getPrinciple().getAWS(), s3User.getArn())) {
                if (checkStatement(statement, bucket, key, methodName, s3User, !s3User.isRootUser())) {
                    result = !s3User.isRootUser();
                }
            }
        }
        return result;
    }

    private boolean checkStatement(Statement statement, String bucket, String key, String methodName, S3User s3User,
                                   boolean isAllow) {
        boolean hasAction = statement.getAction().stream().anyMatch(action -> checkAction(action, methodName));
        boolean isThisResource = statement.getResource().stream().anyMatch(resource ->
                checkResource(resource, bucket, key));
        boolean isThisPrincipal = checkPrincipal(statement.getPrinciple().getAWS(), s3User.getArn());
        boolean isEffectAllow = checkEffect(statement.getEffect());
        return hasAction && isThisResource && isThisPrincipal && (isEffectAllow == isAllow);
    }

    private boolean checkEffect(Statement.EffectType effect) {
        return Statement.EffectType.Allow.equals(effect);
    }

    private boolean checkPrincipal(List<String> patterns, String arn) {
        return patterns.stream().anyMatch(pattern -> match(pattern, arn));
    }

    private boolean checkResource(String pattern, String bucket, String key) {
        String resource = "arn:aws:s3:::" + bucket + (key != null && !key.isEmpty() ? "/" + key : "");
        return match(pattern, resource);
    }

    private boolean checkAction(String pattern, String toCheck) {
        return match(pattern, toCheck);
    }

    @Override
    public Optional<byte[]> getBucketPolicyBytes(String bucket) throws S3Exception {
        Optional<BucketPolicy> bucketPolicy = getBucketPolicy(bucket);
        if (bucketPolicy.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(policyDriver.convertBucketPolicy(bucketPolicy.get()));
    }

    @Override
    public Optional<BucketPolicy> getBucketPolicy(String bucket) throws S3Exception {
        return policyDriver.getBucketPolicy(bucket);
    }

    @Override
    public void putBucketPolicy(String bucket, byte[] bytes) throws S3Exception {
        policyDriver.putBucketPolicy(bucket, bytes);
    }

    @Override
    public void isBucketExists(String bucket) throws S3Exception {
        if (!entityDriver.isBucketExists(bucket)) {
            throw S3Exception.build("Bucket does not exist")
                    .setStatus(HttpResponseStatus.NOT_FOUND)
                    .setCode(S3ResponseErrorCodes.NO_SUCH_BUCKET)
                    .setMessage("The specified bucket does not exist")
                    .setResource("1")
                    .setRequestId("1");
        }
    }

    private boolean checkPermission(Function<Permission, Set<String>> methodsGetter, AccessControlPolicy acl,
                                    String methodName, S3User s3User) {
        return acl.getAccessControlList().stream()
                .filter(grant -> (grant.getGrantee().getDisplayName().equals(s3User.getAccountName()) &&
                                  grant.getGrantee().getId().equals(s3User.getCanonicalUserId())))
                .map(Grant::getPermission)
                .map(methodsGetter)
                .flatMap(Collection::stream)
                .distinct()
                .anyMatch(name -> name.equals(methodName));
    }

    @Override
    public boolean checkAclPermission(boolean isBucketAcl, String bucket, String key, String methodName,
                                      S3User s3User) throws S3Exception {
        return (isBucketAcl ?
                checkBucketAclPermission(bucket, methodName, s3User) :
                checkObjectAclPermission(bucket, key, methodName, s3User));
    }

    @Override
    public boolean isOwner(boolean isBucket, String bucket, String key, S3User s3User) throws S3Exception {
        return (isBucket ?
                isOwner(getBucketAcl(bucket), s3User) :
                isOwner(getObjectAcl(bucket, key), s3User));
    }

    private boolean isOwner(AccessControlPolicy acl, S3User s3User) throws S3Exception {
        return acl.getOwner().getDisplayName().equals(s3User.getAccountName()) &&
               acl.getOwner().getId().equals(s3User.getCanonicalUserId());
    }

    private boolean checkBucketAclPermission(String bucket, String methodName, S3User s3User)
            throws S3Exception {
        AccessControlPolicy acl = getBucketAcl(bucket);
        return checkPermission(Permission::getMethodsBucket, acl, methodName, s3User);
    }

    private boolean checkObjectAclPermission(String bucket, String key, String methodName, S3User s3User)
            throws S3Exception {
        AccessControlPolicy acl = getObjectAcl(bucket, key);
        return checkPermission(Permission::getMethodsObject, acl, methodName, s3User);
    }

    @Override
    public AccessControlPolicy getBucketAcl(String bucket) throws S3Exception {
        return aclDriver.getBucketAcl(bucket);
    }

    @Override
    public AccessControlPolicy getObjectAcl(String bucket, String key) throws S3Exception {
        return aclDriver.getObjectAcl(bucket, key);
    }

    @Override
    public void putBucketAcl(String bucket, byte[] bytes) throws S3Exception {
        putBucketAcl(bucket, aclDriver.parseFromBytes(bytes));
    }

    private void putBucketAcl(String bucket, AccessControlPolicy acl) throws S3Exception {
        aclDriver.putBucketAcl(bucket, acl);
    }

    @Override
    public String putObjectAcl(String bucket, String key, byte[] bytes) throws S3Exception {
        return putObjectAcl(bucket, key, aclDriver.parseFromBytes(bytes));
    }

    private String putObjectAcl(String bucket, String key, AccessControlPolicy acl) throws S3Exception {
        return aclDriver.putObjectAcl(bucket, key, acl);
    }

    @Override
    public void createBucket(String bucket, S3User s3User) throws S3Exception {
        entityDriver.createBucket(bucket, s3User);
        putBucketAcl(bucket, createDefaultAccessControlPolicy(s3User));
    }

    @Override
    public S3Object getObject(String bucket, String key, HttpHeaders httpHeaders) throws S3Exception {
        HasMetaData rawS3Object = entityDriver.getObject(bucket, key, httpHeaders);
        Map<String, String> objectMetadata = metadataDriver.getObjectMetadata(bucket, key);
        return rawS3Object.setMetaData(objectMetadata);
    }

    @Override
    public S3Object putObject(String bucket, String key, byte[] bytes, Map<String, String> metadata, S3User s3User)
            throws S3Exception {
        S3Object s3Object = entityDriver.putObject(bucket, key, bytes, metadata);
        metadataDriver.putObjectMetadata(bucket, key, metadata);
        aclDriver.putObjectAcl(bucket, key, createDefaultAccessControlPolicy(s3User));
        return s3Object;
    }

    @Override
    public void deleteObject(String bucket, String key) throws S3Exception {
        entityDriver.deleteObject(bucket, key);
    }

    @Override
    public List<S3Content> getBucketObjects(String bucket) throws S3Exception {
        List<HasMetaData> hasMetaDataObjects = entityDriver.getBucketObjects(bucket);
        return hasMetaDataObjects.stream()
                .map(hasMetaDataObject -> {
                    Map<String, String> metaData = metadataDriver.getObjectMetadata(bucket, hasMetaDataObject.getKey());
                    return hasMetaDataObject.setMetaData(metaData);
                })
                .map(s3Object -> S3Content.builder()
                        .setETag(s3Object.getETag())
                        .setKey(s3Object.getKey().substring(1))
//                        .setLastModified(s3Object.getLastModified()) // TODO
                        .setOwner(aclDriver.getObjectAcl(bucket, s3Object.getKey()).getOwner())
                        .setSize(s3Object.getRawBytes().length)
                        .setStorageClass("none") // TODO
                        .build())
                .collect(Collectors.toList());
    }

    private AccessControlPolicy createDefaultAccessControlPolicy(S3User s3User) {
        return AccessControlPolicy.builder()
                .setOwner(Owner.builder()
                        .setDisplayName(s3User.getAccountName())
                        .setId(s3User.getCanonicalUserId())
                        .build())
                .setAccessControlList(Collections.singletonList(
                        Grant.builder()
                                .setGrantee(Grantee.builder()
                                        .setDisplayName(s3User.getAccountName())
                                        .setId(s3User.getCanonicalUserId())
                                        .setType("Canonical User") // TODO enum (Canonical User, Group)
                                        .build())
                                .setPermission(Permission.FULL_CONTROL)
                                .build()
                ))
                .build();
    }

    private boolean match(String first, String second)
    {
        if (first.length() == 0 && second.length() == 0)
            return true;

        if (first.length() > 1 && first.charAt(0) == '*' &&
                second.length() == 0)
            return false;

        if ((first.length() != 0 && second.length() != 0 &&
                        first.charAt(0) == second.charAt(0)))
            return match(first.substring(1), second.substring(1));

        if (first.length() > 0 && first.charAt(0) == '*')
            return match(first.substring(1), second) ||
                   match(first, second.substring(1));
        return false;
    }
}
