package com.thorinhood.drivers.main;

import com.thorinhood.data.*;
import com.thorinhood.data.acl.*;
import com.thorinhood.data.multipart.Part;
import com.thorinhood.data.policy.BucketPolicy;
import com.thorinhood.data.policy.Statement;
import com.thorinhood.data.results.GetBucketsResult;
import com.thorinhood.data.results.ListBucketResult;
import com.thorinhood.data.s3object.HasMetaData;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import com.thorinhood.data.s3object.S3ObjectETag;
import com.thorinhood.drivers.FileDriver;
import com.thorinhood.drivers.lock.EntityLocker;
import com.thorinhood.drivers.acl.AclDriver;
import com.thorinhood.drivers.entity.EntityDriver;
import com.thorinhood.drivers.metadata.FileMetadataDriver;
import com.thorinhood.drivers.metadata.MetadataDriver;
import com.thorinhood.drivers.principal.PolicyDriver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.Pair;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

public class S3FileDriverImpl implements S3Driver {

    private final MetadataDriver metadataDriver;
    private final AclDriver aclDriver;
    private final PolicyDriver policyDriver;
    private final EntityDriver entityDriver;
    private final EntityLocker entityLocker;
    private final FileDriver fileDriver;

    private static final Logger log = LogManager.getLogger(S3FileDriverImpl.class);

    public S3FileDriverImpl(MetadataDriver metadataDriver, AclDriver aclDriver, PolicyDriver policyDriver,
                            EntityDriver entityDriver, FileDriver fileDriver, EntityLocker entityLocker) {
        this.metadataDriver = metadataDriver;
        this.aclDriver = aclDriver;
        this.policyDriver = policyDriver;
        this.entityDriver = entityDriver;
        this.entityLocker = entityLocker;
        this.fileDriver = fileDriver;
    }

    @Override
    public boolean checkBucketPolicy(S3FileBucketPath s3FileBucketPath, String key, String methodName, S3User s3User)
            throws S3Exception {
        fileDriver.checkBucket(s3FileBucketPath);
        String policyFilePath = s3FileBucketPath.getPathToBucketPolicyFile();
        if (!fileDriver.isFileExists(policyFilePath)) {
            return s3User.isRootUser();
        }
        Optional<BucketPolicy> bucketPolicy = entityLocker.readMeta(
                s3FileBucketPath.getPathToBucket(),
                s3FileBucketPath.getPathToBucketMetadataFolder(),
                policyFilePath,
                () -> getBucketPolicy(s3FileBucketPath)
        );
        if (bucketPolicy.isEmpty()) {
            return s3User.isRootUser();
        }
        boolean result = s3User.isRootUser();
        for (Statement statement : bucketPolicy.get().getStatements()) {
            if (checkPrincipal(statement.getPrinciple().getAWS(), s3User.getArn())) {
                if (checkStatement(statement, s3FileBucketPath.getBucket(), key, methodName, s3User,
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
    public Optional<byte[]> getBucketPolicyBytes(S3FileBucketPath s3FileBucketPath) throws S3Exception {
        Optional<BucketPolicy> bucketPolicy = entityLocker.readMeta(
                s3FileBucketPath.getPathToBucket(),
                s3FileBucketPath.getPathToBucketMetadataFolder(),
                s3FileBucketPath.getPathToBucketPolicyFile(),
                () -> getBucketPolicy(s3FileBucketPath)
        );
        if (bucketPolicy.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(policyDriver.convertBucketPolicy(bucketPolicy.get()));
    }

    @Override
    public Optional<BucketPolicy> getBucketPolicy(S3FileBucketPath s3FileBucketPath) throws S3Exception {
        return entityLocker.readMeta(
                s3FileBucketPath.getPathToBucket(),
                s3FileBucketPath.getPathToBucketMetadataFolder(),
                s3FileBucketPath.getPathToBucketPolicyFile(),
                () -> policyDriver.getBucketPolicy(s3FileBucketPath)
        );
    }

    @Override
    public void putBucketPolicy(S3FileBucketPath s3FileBucketPath, byte[] bytes) throws S3Exception {
        entityLocker.writeMeta(
                s3FileBucketPath.getPathToBucket(),
                s3FileBucketPath.getPathToBucketMetadataFolder(),
                s3FileBucketPath.getPathToBucketPolicyFile(),
                () -> policyDriver.putBucketPolicy(s3FileBucketPath, bytes)
        );
    }

    @Override
    public void isBucketExists(S3FileBucketPath s3FileBucketPath) throws S3Exception {
        fileDriver.checkBucket(s3FileBucketPath);
    }

    @Override
    public boolean checkAclPermission(boolean isBucketAcl, S3FileObjectPath s3FileObjectPath, String methodName,
                                      S3User s3User) throws S3Exception {
        if (isBucketAcl) {
            AccessControlPolicy acl = getBucketAcl(s3FileObjectPath);
            return checkPermission(Permission::getMethodsBucket, acl, methodName, s3User);
        } else {
            fileDriver.checkObject(s3FileObjectPath);
            AccessControlPolicy acl = getObjectAcl(s3FileObjectPath);
            return checkPermission(Permission::getMethodsObject, acl, methodName, s3User);
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
    public boolean isOwner(boolean isBucket, S3FileObjectPath s3FileObjectPath, S3User s3User) throws S3Exception {
        return (isBucket ?
                isOwner(getBucketAcl(s3FileObjectPath), s3User) :
                isOwner(getObjectAcl(s3FileObjectPath), s3User));
    }

    private boolean isOwner(AccessControlPolicy acl, S3User s3User) throws S3Exception {
        return acl.getOwner().getDisplayName().equals(s3User.getAccountName()) &&
               acl.getOwner().getId().equals(s3User.getCanonicalUserId());
    }

    @Override
    public AccessControlPolicy getBucketAcl(S3FileBucketPath s3FileBucketPath) throws S3Exception {
        return entityLocker.readMeta(
                s3FileBucketPath.getPathToBucket(),
                s3FileBucketPath.getPathToBucketMetadataFolder(),
                s3FileBucketPath.getPathToBucketAclFile(),
                () -> aclDriver.getBucketAcl(s3FileBucketPath)
        );
    }

    @Override
    public AccessControlPolicy getObjectAcl(S3FileObjectPath s3FileObjectPath) throws S3Exception {
        fileDriver.checkObject(s3FileObjectPath);
        return entityLocker.readMeta(
                s3FileObjectPath.getPathToBucket(),
                s3FileObjectPath.getPathToObjectMetadataFolder(),
                s3FileObjectPath.getPathToObjectAclFile(),
                () -> aclDriver.getObjectAcl(s3FileObjectPath)
        );
    }

    @Override
    public void putBucketAcl(S3FileBucketPath s3FileBucketPath, byte[] bytes) throws S3Exception {
        AccessControlPolicy acl = aclDriver.parseFromBytes(bytes);
        entityLocker.writeMeta(
            s3FileBucketPath.getPathToBucket(),
            s3FileBucketPath.getPathToBucketMetadataFolder(),
            s3FileBucketPath.getPathToBucketAclFile(),
            () -> aclDriver.putBucketAcl(s3FileBucketPath, acl)
        );
    }

    @Override
    public String putObjectAcl(S3FileObjectPath s3FileObjectPath, byte[] bytes) throws S3Exception {
        fileDriver.checkObject(s3FileObjectPath);
        AccessControlPolicy acl = aclDriver.parseFromBytes(bytes);
        return entityLocker.writeMeta(
                s3FileObjectPath.getPathToBucket(),
                s3FileObjectPath.getPathToObjectMetadataFolder(),
                s3FileObjectPath.getPathToObjectAclFile(),
                () -> aclDriver.putObjectAcl(s3FileObjectPath, acl)
        );
    }

    @Override
    public void createBucket(S3FileBucketPath s3FileBucketPath, S3User s3User) throws S3Exception {
        entityLocker.writeBucket(
            s3FileBucketPath.getPathToBucket(),
            () -> {
                entityDriver.createBucket(s3FileBucketPath, s3User);
                fileDriver.createFolder(s3FileBucketPath.getPathToBucketMetadataFolder());
                aclDriver.putBucketAcl(s3FileBucketPath, createDefaultAccessControlPolicy(s3User));
            }
        );
    }

    @Override
    public S3Object getObject(S3FileObjectPath s3FileObjectPath, HttpHeaders httpHeaders) throws S3Exception {
        fileDriver.checkObject(s3FileObjectPath);
        return entityLocker.readObject(
            s3FileObjectPath.getPathToBucket(),
            s3FileObjectPath.getPathToObjectMetadataFolder(),
            s3FileObjectPath.getPathToObjectMetaFile(),
            s3FileObjectPath.getPathToObject(),
            () -> {
                Map<String, String> objectMetadata = metadataDriver.getObjectMetadata(s3FileObjectPath);
                HasMetaData rawS3Object = entityDriver.getObject(s3FileObjectPath,
                        objectMetadata.get(FileMetadataDriver.ETAG), httpHeaders);
                objectMetadata.remove(FileMetadataDriver.ETAG);
                return rawS3Object.setMetaData(objectMetadata);
            }
        );
    }

    @Override
    public S3Object putObject(S3FileObjectPath s3FileObjectPath, byte[] bytes, Map<String, String> metadata,
                              S3User s3User) throws S3Exception {
        return entityLocker.writeObject(
            s3FileObjectPath.getPathToBucket(),
            s3FileObjectPath.getPathToObjectMetadataFolder(),
            s3FileObjectPath.getPathToObjectMetaFile(),
            s3FileObjectPath.getPathToObjectAclFile(),
            s3FileObjectPath.getPathToObject(),
            () -> {
                fileDriver.createFolder(s3FileObjectPath.getPathToObjectMetadataFolder());
                S3Object s3Object = entityDriver.putObject(s3FileObjectPath, bytes, metadata);
                metadataDriver.putObjectMetadata(s3FileObjectPath, metadata, s3Object.getETag());
                aclDriver.putObjectAcl(s3FileObjectPath, createDefaultAccessControlPolicy(s3User));
                return s3Object;
            }
        );
    }

    @Override
    public void deleteObject(S3FileObjectPath s3FileObjectPath) throws S3Exception {
        fileDriver.checkObject(s3FileObjectPath);
        entityLocker.deleteObject(
            s3FileObjectPath.getPathToBucket(),
            s3FileObjectPath.getPathToObjectMetadataFolder(),
            s3FileObjectPath.getPathToObjectMetaFile(),
            s3FileObjectPath.getPathToObjectAclFile(),
            s3FileObjectPath.getPathToObject(),
            () -> entityDriver.deleteObject(s3FileObjectPath)
        );
    }

    @Override
    public void deleteBucket(S3FileBucketPath s3FileBucketPath) throws S3Exception {
        entityLocker.writeBucket(
            s3FileBucketPath.getPathToBucket(),
            () -> entityDriver.deleteBucket(s3FileBucketPath)
        );
    }

    @Override
    public ListBucketResult getBucketObjects(GetBucketObjects getBucketObjects) throws S3Exception {
        Pair<Pair<List<S3FileObjectPath>, Boolean>, String> result = entityDriver.getBucketObjects(getBucketObjects);
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        List<Future<S3ObjectETag>> s3objectETagFutures = result.getFirst().getFirst().stream()
                .map(s3FileObjectPath -> executorService.submit(() -> {
                    Map<String, String> metaData = entityLocker.readMeta(
                        s3FileObjectPath.getPathToBucket(),
                        s3FileObjectPath.getPathToObjectMetadataFolder(),
                        s3FileObjectPath.getPathToObjectMetaFile(),
                        () -> metadataDriver.getObjectMetadata(s3FileObjectPath)
                    );
                    return new S3ObjectETag(metaData.get(FileMetadataDriver.ETAG), s3FileObjectPath);
                }))
                .collect(Collectors.toList());
        List<S3ObjectETag> s3ObjectETags = new ArrayList<>();
        for (Future<S3ObjectETag> s3ObjectETagFuture : s3objectETagFutures) {
            try {
                s3ObjectETags.add(s3ObjectETagFuture.get());
            } catch (InterruptedException | ExecutionException e) {
                throw S3Exception.INTERNAL_ERROR(e)
                        .setResource("1")
                        .setRequestId("1");
            }
        }
        List<Future<S3Content>> s3ContentFutures = s3ObjectETags.stream()
                .map(s3ObjectETag -> executorService.submit(() -> S3Content.builder()
                        .setETag(s3ObjectETag.getETag())
                        .setKey(s3ObjectETag.getS3FileObjectPath().getKey())
                        .setLastModified(DateTimeUtil.parseDateTimeISO(s3ObjectETag.getFile()))
                        .setOwner(entityLocker.readMeta(
                                s3ObjectETag.getS3FileObjectPath().getPathToBucket(),
                                s3ObjectETag.getS3FileObjectPath().getPathToObjectMetadataFolder(),
                                s3ObjectETag.getS3FileObjectPath().getPathToObjectAclFile(),
                                () -> aclDriver.getObjectAcl(s3ObjectETag.getS3FileObjectPath()).getOwner()))
                        .setSize(s3ObjectETag.getFile().length())
                        .setStorageClass("STANDART")
                        .build()))
                .collect(Collectors.toList());
        List<S3Content> s3Contents = new ArrayList<>();
        for (Future<S3Content> s3ContentFuture : s3ContentFutures) {
            try {
                s3Contents.add(s3ContentFuture.get());
            } catch (InterruptedException | ExecutionException e) {
                throw S3Exception.INTERNAL_ERROR(e)
                        .setResource("1")
                        .setRequestId("1");
            }
        }
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

    @Override
    public String createMultipartUpload(S3FileObjectPath s3FileObjectPath) throws S3Exception {
        fileDriver.checkObject(s3FileObjectPath);
        String uploadId = DigestUtils.md5Hex(DateTimeUtil.currentDateTime() + new Random().nextLong() +
                new File(s3FileObjectPath.getPathToObject()).getAbsolutePath());
        entityLocker.createUpload(
            s3FileObjectPath.getPathToBucket(),
            s3FileObjectPath.getPathToObjectMetadataFolder(),
            s3FileObjectPath.getPathToObjectUploadFolder(uploadId),
            () -> entityDriver.createMultipartUpload(s3FileObjectPath, uploadId)
        );
        return uploadId;
    }

    @Override
    public void abortMultipartUpload(S3FileObjectPath s3FileObjectPath, String uploadId) throws S3Exception {
        fileDriver.checkObject(s3FileObjectPath);
        if (uploadId == null) {
            return;
        }
        if (!fileDriver.isFolderExists(s3FileObjectPath.getPathToObjectUploadFolder(uploadId))) {
            return;
        }
        entityLocker.deleteUpload(
            s3FileObjectPath.getPathToBucket(),
            s3FileObjectPath.getPathToObjectMetadataFolder(),
            uploadId,
            () -> entityDriver.abortMultipartUpload(s3FileObjectPath, uploadId)
        );
    }

    @Override
    public String putUploadPart(S3FileObjectPath s3FileObjectPath, String uploadId, int partNumber, byte[] bytes)
            throws S3Exception {
        fileDriver.checkObject(s3FileObjectPath);
        String pathToUpload = s3FileObjectPath.getPathToObjectUploadFolder(uploadId);
        if (!fileDriver.isFolderExists(pathToUpload)) {
            throw S3Exception.NO_SUCH_UPLOAD(uploadId);
        }
        return entityLocker.writeUpload(
            s3FileObjectPath.getPathToBucket(),
            s3FileObjectPath.getPathToObjectMetadataFolder(),
            pathToUpload,
            s3FileObjectPath.getPathToObjectUploadPart(uploadId, partNumber),
            () -> entityDriver.putUploadPart(s3FileObjectPath, uploadId, partNumber, bytes)
        );
    }

    @Override
    public String completeMultipartUpload(S3FileObjectPath s3FileObjectPath, String uploadId, List<Part> parts,
                                          S3User s3User) throws S3Exception {
        fileDriver.checkObject(s3FileObjectPath);
        if (parts == null || parts.size() == 0) {
            throw S3Exception.build("No parts to complete multipart upload")
                    .setStatus(HttpResponseStatus.BAD_REQUEST)
                    .setCode(S3ResponseErrorCodes.INVALID_REQUEST)
                    .setMessage("No parts to complete multipart upload")
                    .setResource("1")
                    .setRequestId("1");
        }
        return entityLocker.completeUpload(
                s3FileObjectPath.getPathToBucket(),
                s3FileObjectPath.getPathToObjectMetadataFolder(),
                s3FileObjectPath.getPathToObjectUploadFolder(uploadId),
                () -> {
                    String eTag = entityDriver.completeMultipartUpload(s3FileObjectPath, uploadId, parts);
                    metadataDriver.putObjectMetadata(s3FileObjectPath, Map.of(), eTag);
                    aclDriver.putObjectAcl(s3FileObjectPath, createDefaultAccessControlPolicy(s3User));
                    return eTag;
                }
        );
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
