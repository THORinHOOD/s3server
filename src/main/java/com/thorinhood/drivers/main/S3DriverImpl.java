package com.thorinhood.drivers.main;

import com.thorinhood.data.*;
import com.thorinhood.data.acl.*;
import com.thorinhood.data.policy.BucketPolicy;
import com.thorinhood.data.policy.Statement;
import com.thorinhood.data.s3object.HasMetaData;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import com.thorinhood.drivers.acl.AclDriver;
import com.thorinhood.drivers.entity.EntityDriver;
import com.thorinhood.drivers.metadata.MetadataDriver;
import com.thorinhood.drivers.principal.PolicyDriver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.Pair;
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
    public boolean checkBucketPolicy(S3BucketPath s3BucketPath, String key, String methodName, S3User s3User) throws S3Exception {
        Optional<BucketPolicy> bucketPolicy = getBucketPolicy(s3BucketPath);
        if (bucketPolicy.isEmpty()) {
            return s3User.isRootUser();
        }
        boolean result = s3User.isRootUser();
        for (Statement statement : bucketPolicy.get().getStatements()) {
            if (checkPrincipal(statement.getPrinciple().getAWS(), s3User.getArn())) {
                if (checkStatement(statement, s3BucketPath.getBucket(), key, methodName, s3User,
                        !s3User.isRootUser())) {
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
    public Optional<byte[]> getBucketPolicyBytes(S3BucketPath s3BucketPath) throws S3Exception {
        Optional<BucketPolicy> bucketPolicy = getBucketPolicy(s3BucketPath);
        if (bucketPolicy.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(policyDriver.convertBucketPolicy(bucketPolicy.get()));
    }

    @Override
    public Optional<BucketPolicy> getBucketPolicy(S3BucketPath s3BucketPath) throws S3Exception {
        return policyDriver.getBucketPolicy(s3BucketPath);
    }

    @Override
    public void putBucketPolicy(S3BucketPath s3BucketPath, byte[] bytes) throws S3Exception {
        policyDriver.putBucketPolicy(s3BucketPath, bytes);
    }

    @Override
    public void isBucketExists(S3BucketPath s3BucketPath) throws S3Exception {
        if (!entityDriver.isBucketExists(s3BucketPath)) {
            throw S3Exception.build("Bucket does not exist")
                    .setStatus(HttpResponseStatus.NOT_FOUND)
                    .setCode(S3ResponseErrorCodes.NO_SUCH_BUCKET)
                    .setMessage("The specified bucket does not exist")
                    .setResource("1")
                    .setRequestId("1");
        }
    }

    @Override
    public void isObjectExists(S3ObjectPath s3ObjectPath) throws S3Exception {
        if (!entityDriver.isObjectExists(s3ObjectPath)) {
            throw S3Exception.build("Object does not exist")
                    .setStatus(HttpResponseStatus.NOT_FOUND)
                    .setCode(S3ResponseErrorCodes.NO_SUCH_KEY)
                    .setMessage("The specified object does not exist")
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
    public boolean checkAclPermission(boolean isBucketAcl, S3ObjectPath s3ObjectPath, String methodName, S3User s3User)
            throws S3Exception {
        if (isBucketAcl) {
            AccessControlPolicy acl = getBucketAcl(s3ObjectPath);
            return checkPermission(Permission::getMethodsBucket, acl, methodName, s3User);
        } else {
            isObjectExists(s3ObjectPath);
            AccessControlPolicy acl = getObjectAcl(s3ObjectPath);
            return checkPermission(Permission::getMethodsObject, acl, methodName, s3User);
        }
    }

    @Override
    public boolean isOwner(boolean isBucket, S3ObjectPath s3ObjectPath, S3User s3User) throws S3Exception {
        return (isBucket ?
                isOwner(getBucketAcl(s3ObjectPath), s3User) :
                isOwner(getObjectAcl(s3ObjectPath), s3User));
    }

    private boolean isOwner(AccessControlPolicy acl, S3User s3User) throws S3Exception {
        return acl.getOwner().getDisplayName().equals(s3User.getAccountName()) &&
               acl.getOwner().getId().equals(s3User.getCanonicalUserId());
    }

    @Override
    public AccessControlPolicy getBucketAcl(S3BucketPath s3BucketPath) throws S3Exception {
        return aclDriver.getBucketAcl(s3BucketPath);
    }

    @Override
    public AccessControlPolicy getObjectAcl(S3ObjectPath s3ObjectPath) throws S3Exception {
        return aclDriver.getObjectAcl(s3ObjectPath);
    }

    @Override
    public void putBucketAcl(S3BucketPath s3BucketPath, byte[] bytes) throws S3Exception {
        putBucketAcl(s3BucketPath, aclDriver.parseFromBytes(bytes));
    }

    private void putBucketAcl(S3BucketPath s3BucketPath, AccessControlPolicy acl) throws S3Exception {
        aclDriver.putBucketAcl(s3BucketPath, acl);
    }

    @Override
    public String putObjectAcl(S3ObjectPath s3ObjectPath, byte[] bytes) throws S3Exception {
        return putObjectAcl(s3ObjectPath, aclDriver.parseFromBytes(bytes));
    }

    private String putObjectAcl(S3ObjectPath s3ObjectPath, AccessControlPolicy acl) throws S3Exception {
        return aclDriver.putObjectAcl(s3ObjectPath, acl);
    }

    @Override
    public void createBucket(S3BucketPath s3BucketPath, S3User s3User) throws S3Exception {
        entityDriver.createBucket(s3BucketPath, s3User);
        putBucketAcl(s3BucketPath, createDefaultAccessControlPolicy(s3User));
    }

    @Override
    public S3Object getObject(S3ObjectPath s3ObjectPath, HttpHeaders httpHeaders) throws S3Exception {
        HasMetaData rawS3Object = entityDriver.getObject(s3ObjectPath, httpHeaders);
        Map<String, String> objectMetadata = metadataDriver.getObjectMetadata(s3ObjectPath);
        return rawS3Object.setMetaData(objectMetadata);
    }

    @Override
    public S3Object putObject(S3ObjectPath s3ObjectPath, byte[] bytes, Map<String, String> metadata, S3User s3User)
            throws S3Exception {
        S3Object s3Object = entityDriver.putObject(s3ObjectPath, bytes, metadata);
        metadataDriver.putObjectMetadata(s3ObjectPath, metadata);
        aclDriver.putObjectAcl(s3ObjectPath, createDefaultAccessControlPolicy(s3User));
        return s3Object;
    }

    @Override
    public void deleteObject(S3ObjectPath s3ObjectPath) throws S3Exception {
        entityDriver.deleteObject(s3ObjectPath);
    }

    @Override
    public void deleteBucket(S3BucketPath s3BucketPath) throws S3Exception {
        entityDriver.deleteBucket(s3BucketPath);
    }

    @Override
    public ListBucketResult getBucketObjects(GetBucketObjects getBucketObjects) throws S3Exception {
        Pair<Pair<List<HasMetaData>, Boolean>, String> result = entityDriver.getBucketObjects(getBucketObjects);
        List<S3Content> s3Contents = result.getFirst().getFirst().stream()
                .map(hasMetaDataObject -> {
                    Map<String, String> metaData = metadataDriver.getObjectMetadata(hasMetaDataObject.getS3Path());
                    return hasMetaDataObject.setMetaData(metaData); // TODO
                })
                .map(s3Object -> S3Content.builder()
                        .setETag(s3Object.getETag())
                        .setKey(s3Object.getS3Path().getKey())
                        .setLastModified(DateTimeUtil.parseDateTimeISO(s3Object.getFile()))
                        .setOwner(aclDriver.getObjectAcl(s3Object.getS3Path()).getOwner())
                        .setSize(s3Object.getRawBytes().length)
                        .setStorageClass("STANDART") // TODO
                        .build())
                .collect(Collectors.toList());
        return ListBucketResult.builder()
                .setMaxKeys(getBucketObjects.getMaxKeys())
                .setName(getBucketObjects.getBucket())
                .setContents(s3Contents)
                .setPrefix(getBucketObjects.getPrefix())
                .setStartAfter(getBucketObjects.getStartAfter())
                .setIsTruncated(result.getFirst().getSecond())
                .setKeyCount(s3Contents.size())
                .setContinuationToken(getBucketObjects.getContinuationToken())
                .setNextContinuationToken(result.getSecond())
                .build();
    }

    @Override
    public GetBucketsResult getBuckets(S3User s3User) throws S3Exception {
        List<Pair<String, String>> buckets = entityDriver.getBuckets(s3User);
        return GetBucketsResult.builder()
                .setBuckets(buckets)
                .setOwner(Owner.builder()
                        .setDisplayName(s3User.getAccountName())
                        .setId(s3User.getCanonicalUserId())
                        .build())
                .build();
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
