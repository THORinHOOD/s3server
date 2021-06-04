package com.thorinhood.drivers.main;

import com.thorinhood.data.*;
import com.thorinhood.data.acl.*;
import com.thorinhood.data.list.eventual.ListBucketResult;
import com.thorinhood.data.list.raw.ListBucketResultRaw;
import com.thorinhood.data.list.request.GetBucketObjects;
import com.thorinhood.data.list.request.GetBucketObjectsV2;
import com.thorinhood.data.multipart.Part;
import com.thorinhood.data.policy.BucketPolicy;
import com.thorinhood.data.policy.Statement;
import com.thorinhood.data.results.CopyObjectResult;
import com.thorinhood.data.results.GetBucketsResult;
import com.thorinhood.data.list.eventual.ListBucketV2Result;
import com.thorinhood.data.list.raw.ListBucketV2ResultRaw;
import com.thorinhood.data.s3object.HasMetaData;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import com.thorinhood.data.s3object.S3ObjectETag;
import com.thorinhood.drivers.FileDriver;
import com.thorinhood.drivers.lock.EntityLockDriver;
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
import java.io.IOException;
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
    private final EntityLockDriver entityLockDriver;
    private final FileDriver fileDriver;

    private static final Logger log = LogManager.getLogger(S3FileDriverImpl.class);

    public S3FileDriverImpl(MetadataDriver metadataDriver, AclDriver aclDriver, PolicyDriver policyDriver,
                            EntityDriver entityDriver, FileDriver fileDriver, EntityLockDriver entityLockDriver) {
        this.metadataDriver = metadataDriver;
        this.aclDriver = aclDriver;
        this.policyDriver = policyDriver;
        this.entityDriver = entityDriver;
        this.entityLockDriver = entityLockDriver;
        this.fileDriver = fileDriver;
    }

    @Override
    public Optional<Boolean> checkBucketPolicy(S3FileBucketPath s3FileBucketPath, String key, String methodName,
                                               S3User s3User) throws S3Exception {
        fileDriver.checkBucket(s3FileBucketPath);
        String policyFilePath = s3FileBucketPath.getPathToBucketPolicyFile();
        if (!fileDriver.isFileExists(policyFilePath)) {
            return Optional.empty();
        }
        Optional<BucketPolicy> bucketPolicy = entityLockDriver.readMeta(
                s3FileBucketPath.getPathToBucket(),
                s3FileBucketPath.getPathToBucketMetadataFolder(),
                policyFilePath,
                () -> getBucketPolicy(s3FileBucketPath)
        );
        if (bucketPolicy.isEmpty()) {
            return Optional.empty();
        }
        for (Statement statement : bucketPolicy.get().getStatements()) {
            Optional<Boolean> checkResult = checkStatement(statement, s3FileBucketPath.getBucket(), key,
                    methodName, s3User);
            if (checkResult.isPresent()) {
                return checkResult;
            }
        }
        return Optional.empty();
    }

    private Optional<Boolean> checkStatement(Statement statement, String bucket, String key, String methodName,
                                             S3User s3User) {
        boolean isThisPrincipal = checkPrincipal(statement.getPrinciple().getAWS(), s3User.getArn());
        if (!isThisPrincipal) {
            return Optional.empty();
        }
        boolean hasAction = statement.getAction().stream().anyMatch(action -> checkAction(action, methodName));
        if (!hasAction) {
            return Optional.empty();
        }
        boolean isThisResource = statement.getResource().stream().anyMatch(resource ->
                checkResource(resource, bucket, key));
        if (!isThisResource) {
            return Optional.empty();
        }
        return Optional.of(checkEffect(statement.getEffect()));
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
        Optional<BucketPolicy> bucketPolicy = entityLockDriver.readMeta(
                s3FileBucketPath.getPathToBucket(),
                s3FileBucketPath.getPathToBucketMetadataFolder(),
                s3FileBucketPath.getPathToBucketPolicyFile(),
                () -> getBucketPolicy(s3FileBucketPath)
        );
        if (bucketPolicy.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(policyDriver.convertBucketPolicy(s3FileBucketPath, bucketPolicy.get()));
    }

    @Override
    public Optional<BucketPolicy> getBucketPolicy(S3FileBucketPath s3FileBucketPath) throws S3Exception {
        return entityLockDriver.readMeta(
                s3FileBucketPath.getPathToBucket(),
                s3FileBucketPath.getPathToBucketMetadataFolder(),
                s3FileBucketPath.getPathToBucketPolicyFile(),
                () -> policyDriver.getBucketPolicy(s3FileBucketPath)
        );
    }

    @Override
    public void putBucketPolicy(S3FileBucketPath s3FileBucketPath, byte[] bytes) throws S3Exception {
        entityLockDriver.writeMeta(
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
        return entityLockDriver.readMeta(
                s3FileBucketPath.getPathToBucket(),
                s3FileBucketPath.getPathToBucketMetadataFolder(),
                s3FileBucketPath.getPathToBucketAclFile(),
                () -> aclDriver.getBucketAcl(s3FileBucketPath)
        );
    }

    @Override
    public AccessControlPolicy getObjectAcl(S3FileObjectPath s3FileObjectPath) throws S3Exception {
        fileDriver.checkObject(s3FileObjectPath);
        return entityLockDriver.readMeta(
                s3FileObjectPath.getPathToBucket(),
                s3FileObjectPath.getPathToObjectMetadataFolder(),
                s3FileObjectPath.getPathToObjectAclFile(),
                () -> aclDriver.getObjectAcl(s3FileObjectPath)
        );
    }

    @Override
    public void putBucketAcl(S3FileBucketPath s3FileBucketPath, byte[] bytes) throws S3Exception {
        AccessControlPolicy acl = aclDriver.parseFromBytes(bytes);
        entityLockDriver.writeMeta(
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
        return entityLockDriver.writeMeta(
                s3FileObjectPath.getPathToBucket(),
                s3FileObjectPath.getPathToObjectMetadataFolder(),
                s3FileObjectPath.getPathToObjectAclFile(),
                () -> aclDriver.putObjectAcl(s3FileObjectPath, acl)
        );
    }

    @Override
    public void createBucket(S3FileBucketPath s3FileBucketPath, S3User s3User) throws S3Exception {
        File bucketFile = new File(s3FileBucketPath.getPathToBucket());
        if (bucketFile.exists() && bucketFile.isDirectory()) {
            boolean isOwner = isOwner(true, (S3FileObjectPath) s3FileBucketPath, s3User);
            if (!isOwner) {
                throw S3Exception.BUCKET_ALREADY_EXISTS();
            } else {
                throw S3Exception.BUCKET_ALREADY_OWNED_BY_YOU(bucketFile.getAbsolutePath());
            }
        }
        entityLockDriver.writeBucket(
            s3FileBucketPath,
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
        return entityLockDriver.readObject(
            s3FileObjectPath,
            () -> {
                Map<String, String> objectMetadata = metadataDriver.getObjectMetadata(s3FileObjectPath);
                HasMetaData rawS3Object = entityDriver.getObject(s3FileObjectPath,
                        objectMetadata.get(FileMetadataDriver.ETAG), httpHeaders, false);
                objectMetadata.remove(FileMetadataDriver.ETAG);
                return rawS3Object.setMetaData(objectMetadata);
            }
        );
    }

    @Override
    public CopyObjectResult copyObject(S3FileObjectPath source, S3FileObjectPath target, HttpHeaders httpHeaders,
                                       S3User s3User) throws S3Exception {
        S3Object sourceObject = entityLockDriver.readObject(
            source,
            () -> {
                Map<String, String> metadata = metadataDriver.getObjectMetadata(source);
                String sourceETag = metadata.get(FileMetadataDriver.ETAG);
                metadata.remove(FileMetadataDriver.ETAG);
                return entityDriver.getObject(source, sourceETag, httpHeaders, true)
                        .setMetaData(metadata);
            }
        );
        S3Object targetObject = putObject(target, sourceObject.getRawBytes(), sourceObject.getMetaData(), s3User);
        return CopyObjectResult.builder()
                .setETag("\"" + targetObject.getETag() + "\"")
                .setLastModified(DateTimeUtil.parseDateTimeISO(targetObject.getFile()))
                .build();
    }

    @Override
    public S3Object headObject(S3FileObjectPath s3FileObjectPath, HttpHeaders httpHeaders) throws S3Exception {
        fileDriver.checkObject(s3FileObjectPath);
        return entityLockDriver.readObject(
                s3FileObjectPath,
                () -> {
                    Map<String, String> objectMetadata = metadataDriver.getObjectMetadata(s3FileObjectPath);
                    HasMetaData rawS3Object = entityDriver.headObject(s3FileObjectPath,
                            objectMetadata.get(FileMetadataDriver.ETAG), httpHeaders);
                    objectMetadata.remove(FileMetadataDriver.ETAG);
                    return rawS3Object.setMetaData(objectMetadata);
                }
        );
    }

    @Override
    public S3Object putObject(S3FileObjectPath s3FileObjectPath, byte[] bytes, Map<String, String> metadata,
                              S3User s3User) throws S3Exception {
        return entityLockDriver.writeObject(
            s3FileObjectPath,
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
        entityLockDriver.deleteObject(s3FileObjectPath, () -> entityDriver.deleteObject(s3FileObjectPath));
    }

    @Override
    public void deleteBucket(S3FileBucketPath s3FileBucketPath) throws S3Exception {
        entityLockDriver.writeBucket(s3FileBucketPath, () -> entityDriver.deleteBucket(s3FileBucketPath));
    }

    @Override
    public ListBucketV2Result getBucketObjectsV2(S3FileBucketPath s3FileBucketPath,
                                                 GetBucketObjectsV2 getBucketObjectsV2) throws S3Exception {
        ListBucketV2ResultRaw rawResult = entityDriver.getBucketObjectsV2(getBucketObjectsV2);
        List<S3Content> s3Contents = makeContents(rawResult.getS3FileObjectsPaths());
        return ListBucketV2Result.builder()
                .setMaxKeys(getBucketObjectsV2.getMaxKeys())
                .setName(getBucketObjectsV2.getBucket())
                .setContents(s3Contents)
                .setPrefix(getBucketObjectsV2.getPrefix())
                .setStartAfter(getBucketObjectsV2.getStartAfter())
                .setIsTruncated(rawResult.isTruncated())
                .setKeyCount(rawResult.getKeyCount())
                .setContinuationToken(getBucketObjectsV2.getContinuationToken())
                .setNextContinuationToken(rawResult.getNextContinuationToken())
                .setDelimiter(getBucketObjectsV2.getDelimiter())
                .setCommonPrefixes(rawResult.getCommonPrefixes())
                .build();
    }

    @Override
    public ListBucketResult getBucketObjects(GetBucketObjects getBucketObjects) throws S3Exception {
        ListBucketResultRaw rawResult = entityDriver.getBucketObjects(getBucketObjects);
        List<S3Content> s3Contents = makeContents(rawResult.getS3FileObjectsPaths());
        return ListBucketResult.builder()
                .setMaxKeys(getBucketObjects.getMaxKeys())
                .setName(getBucketObjects.getBucket())
                .setContents(s3Contents)
                .setPrefix(getBucketObjects.getPrefix())
                .setIsTruncated(rawResult.isTruncated())
                .setDelimiter(getBucketObjects.getDelimiter())
                .setCommonPrefixes(rawResult.getCommonPrefixes())
                .setMarker(getBucketObjects.getMarker())
                .setNextMarker(rawResult.getNextMarker())
                .build();
    }

    @Override
    public GetBucketsResult getBuckets(S3User s3User) throws S3Exception {
        List<Pair<S3FileBucketPath, String>> buckets = entityDriver.getBuckets(s3User);
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        List<Future<Pair<S3FileBucketPath, Pair<AccessControlPolicy, String>>>> bucketsWithAclFutures = buckets.stream()
                .map(pair -> executorService.submit(() -> {
                    AccessControlPolicy acl = entityLockDriver.readMeta(
                        pair.getFirst().getPathToBucket(),
                        pair.getFirst().getPathToBucketMetadataFolder(),
                        pair.getFirst().getPathToBucketAclFile(),
                        () -> aclDriver.getBucketAcl(pair.getFirst())
                    );
                    return Pair.of(pair.getFirst(), Pair.of(acl, pair.getSecond()));
                }))
                .collect(Collectors.toList());
        List<Pair<String, String>> bucketsFiltered = new ArrayList<>();
        for (Future<Pair<S3FileBucketPath, Pair<AccessControlPolicy, String>>> bucketWithAclFuture :
                bucketsWithAclFutures) {
            try {
                Pair<S3FileBucketPath, Pair<AccessControlPolicy, String>> bucketWithAcl = bucketWithAclFuture.get();
                AccessControlPolicy acl = bucketWithAcl.getSecond().getFirst();
                if (s3User.getCanonicalUserId().equals(acl.getOwner().getId()) &&
                    s3User.getAccountName().equals(acl.getOwner().getDisplayName())) {
                    bucketsFiltered.add(Pair.of(bucketWithAcl.getFirst().getBucket(),
                            bucketWithAcl.getSecond().getSecond()));
                }
            } catch (InterruptedException | ExecutionException e) {
                throw S3Exception.INTERNAL_ERROR(e);
            }
        }
        return GetBucketsResult.builder()
                .setBuckets(bucketsFiltered)
                .setOwner(Owner.builder()
                        .setDisplayName(s3User.getAccountName())
                        .setId(s3User.getCanonicalUserId())
                        .build())
                .build();
    }

    @Override
    public String createMultipartUpload(S3FileObjectPath s3FileObjectPath, S3User s3User) throws S3Exception {
        String uploadId = DigestUtils.md5Hex(DateTimeUtil.currentDateTime() + new Random().nextLong() +
                new File(s3FileObjectPath.getPathToObject()).getAbsolutePath()) + "_" + s3User.getAccessKey();
        entityLockDriver.createUpload(
            s3FileObjectPath.getPathToBucket(),
            s3FileObjectPath.getPathToObjectMetadataFolder(),
            s3FileObjectPath.getPathToObjectUploadFolder(uploadId),
            () -> entityDriver.createMultipartUpload(s3FileObjectPath, uploadId)
        );
        return uploadId;
    }

    @Override
    public void abortMultipartUpload(S3FileObjectPath s3FileObjectPath, String uploadId) throws S3Exception {
        if (uploadId == null) {
            return;
        }
        if (!fileDriver.isFolderExists(s3FileObjectPath.getPathToObjectUploadFolder(uploadId))) {
            return;
        }
        entityLockDriver.deleteUpload(
            s3FileObjectPath.getPathToBucket(),
            s3FileObjectPath.getPathToObjectMetadataFolder(),
            uploadId,
            () -> entityDriver.abortMultipartUpload(s3FileObjectPath, uploadId)
        );
    }

    @Override
    public String putUploadPart(S3FileObjectPath s3FileObjectPath, String uploadId, int partNumber, byte[] bytes)
            throws S3Exception {
        String pathToUpload = s3FileObjectPath.getPathToObjectUploadFolder(uploadId);
        if (!fileDriver.isFolderExists(pathToUpload)) {
            throw S3Exception.NO_SUCH_UPLOAD(uploadId);
        }
        return entityLockDriver.writeUpload(
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
        if (parts == null || parts.size() == 0) {
            throw S3Exception.builder("No parts to complete multipart upload")
                    .setStatus(HttpResponseStatus.BAD_REQUEST)
                    .setCode(S3ResponseErrorCodes.INVALID_REQUEST)
                    .setMessage("No parts to complete multipart upload")
                    .build();
        }
        File file = new File(s3FileObjectPath.getPathToObject());
        if (!file.exists()) {
            try {
                if (!file.createNewFile()) {
                    throw S3Exception.INTERNAL_ERROR("Can't create file : " + s3FileObjectPath.getPathToObject());
                }
            } catch (IOException exception) {
                throw S3Exception.INTERNAL_ERROR(exception);
            }
        }
        return entityLockDriver.completeUpload(
                s3FileObjectPath,
                s3FileObjectPath.getPathToObjectUploadFolder(uploadId),
                () -> {
                    String eTag = entityDriver.completeMultipartUpload(s3FileObjectPath, uploadId, parts);
                    metadataDriver.putObjectMetadata(s3FileObjectPath, Map.of(), eTag);
                    aclDriver.putObjectAcl(s3FileObjectPath, createDefaultAccessControlPolicy(s3User));
                    return eTag;
                }
        );
    }

    @Override
    public S3FileObjectPath buildPathToObject(String bucketKeyToObject) {
        return fileDriver.buildPathToObject(bucketKeyToObject);
    }

    private List<S3Content> makeContents(List<S3FileObjectPath> s3FileObjectPaths) {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        List<Future<S3ObjectETag>> s3objectETagFutures = s3FileObjectPaths.stream()
                .map(s3FileObjectPath -> executorService.submit(() -> {
                    Map<String, String> metaData = entityLockDriver.readMeta(
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
                throw S3Exception.INTERNAL_ERROR(e);
            }
        }
        List<Future<S3Content>> s3ContentFutures = s3ObjectETags.stream()
                .map(s3ObjectETag -> executorService.submit(() -> S3Content.builder()
                        .setETag("\"" + s3ObjectETag.getETag() + "\"")
                        .setKey(s3ObjectETag.getS3FileObjectPath().getKey())
                        .setLastModified(DateTimeUtil.parseDateTimeISO(s3ObjectETag.getFile()))
                        .setOwner(entityLockDriver.readMeta(
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
                throw S3Exception.INTERNAL_ERROR(e);
            }
        }
        executorService.shutdown();
        return s3Contents;
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
                                        .setType("Canonical User")
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
