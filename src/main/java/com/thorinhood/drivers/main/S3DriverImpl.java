package com.thorinhood.drivers.main;

import com.thorinhood.data.S3User;
import com.thorinhood.data.acl.*;
import com.thorinhood.data.S3Headers;
import com.thorinhood.data.policy.BucketPolicy;
import com.thorinhood.data.policy.Statement;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.data.S3ResponseErrorCodes;
import com.thorinhood.drivers.acl.AclDriver;
import com.thorinhood.drivers.metadata.MetadataDriver;
import com.thorinhood.drivers.principal.PolicyDriver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.selectors.*;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.ParsedRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.text.ParseException;
import java.util.*;
import java.util.function.Function;

public class S3DriverImpl implements S3Driver {

    private final MetadataDriver metadataDriver;
    private final AclDriver aclDriver;
    private final PolicyDriver policyDriver;
    private final Map<String, Selector<String>> strSelectors;
    private final Map<String, Selector<Date>> dateSelectors;


    private static final Logger log = LogManager.getLogger(S3DriverImpl.class);

    public S3DriverImpl(MetadataDriver metadataDriver, AclDriver aclDriver, PolicyDriver policyDriver) {
        this.metadataDriver = metadataDriver;
        this.aclDriver = aclDriver;
        this.policyDriver = policyDriver;
        strSelectors = Map.of(
                S3Headers.IF_MATCH, new IfMatch(),
                S3Headers.IF_NONE_MATCH, new IfNoneMatch()
        );
        dateSelectors = Map.of(
                S3Headers.IF_MODIFIED_SINCE, new IfModifiedSince(),
                S3Headers.IF_UNMODIFIED_SINCE, new IfUnmodifiedSince()
        );
    }

    @Override
    public boolean checkBucketPolicy(String bucket, String key, String methodName, S3User s3User) throws S3Exception {
        Optional<BucketPolicy> bucketPolicy = getBucketPolicy(bucket);
        if (bucketPolicy.isEmpty()) {
            return s3User.isRootUser();
        }
        boolean result = false;
        for (Statement statement : bucketPolicy.get().getStatements()) {
            if (checkPrincipal(statement.getPrinciple().getAWS(), s3User.getArn())) {
                if (checkStatement(statement, bucket, key, methodName, s3User)) {
                    result = true;
                }
            }
        }
        return result;
    }

    private boolean checkStatement(Statement statement, String bucket, String key, String methodName, S3User s3User) {
        boolean hasAction = statement.getAction().stream().anyMatch(action -> checkAction(action, methodName));
        boolean isThisResource = statement.getResource().stream().anyMatch(resource ->
                checkResource(resource, bucket, key));
        boolean isThisPrincipal = checkPrincipal(statement.getPrinciple().getAWS(), s3User.getArn());
        boolean isEffectAllow = checkEffect(statement.getEffect());
        return hasAction && isThisResource && isThisPrincipal && isEffectAllow;
    }

    private boolean checkEffect(Statement.EffectType effect) {
        return Statement.EffectType.Allow.equals(effect);
    }

    private boolean checkPrincipal(List<String> patterns, String arn) {
        return patterns.stream().anyMatch(pattern -> match(pattern, arn));
    }

    private boolean checkResource(String pattern, String bucket, String key) {
        String resource = "arn:aws:s3::" + bucket + (key != null && !key.isEmpty() ? "/" + key : "");
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

    private boolean checkPermission(Function<Permission, Set<String>> methodsGetter, AccessControlPolicy acl,
                                    String methodName, S3User s3User) {
        return acl.getAccessControlList().stream()
                .filter(grant -> (s3User.isRootUser() &&
                                  grant.getGrantee().getDisplayName().equals(s3User.getAccountName()) &&
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
    public void createBucket(String bucket, String basePath, S3User s3User) throws S3Exception {
        String absolutePath = basePath + File.separatorChar + bucket;
        File bucketFile = new File(absolutePath);
        if (bucketFile.exists()) {
            throw S3Exception.build("Bucket already exists : " + absolutePath)
                    .setStatus(HttpResponseStatus.CONFLICT)
                    .setCode(S3ResponseErrorCodes.BUCKET_ALREADY_OWNED_BY_YOU)
                    .setMessage("Your previous request to create the named bucket succeeded and you already own it.")
                    .setResource(File.separatorChar + bucket)
                    .setRequestId("1");
        }
        if (!bucketFile.mkdir()) {
            throw S3Exception.INTERNAL_ERROR("Can't create bucket: " + absolutePath)
                    .setMessage("Can't create bucket")
                    .setResource(File.separatorChar + bucket)
                    .setRequestId("1");
        }
        putBucketAcl(bucket, createDefaultAccessControlPolicy(s3User));
    }

    @Override
    public S3Object getObject(ParsedRequest request, String basePath) throws S3Exception {
        Optional<String> absolutePath = buildPath(basePath, request.getBucket(), request.getKey());
        if (absolutePath.isEmpty()) {
            //TODO
            return null;
        }
        File file = new File(absolutePath.get());
        if (file.isHidden() || !file.exists()) {
            throw S3Exception.build("File not found: " + absolutePath)
                    .setStatus(HttpResponseStatus.NOT_FOUND)
                    .setCode(S3ResponseErrorCodes.NO_SUCH_KEY)
                    .setMessage("The resource you requested does not exist")
                    .setResource(File.separatorChar + request.getBucket() + request.getKey())
                    .setRequestId("1");
        }
        if (!file.isFile()) {
            //TODO
            return null;
        }

        byte[] bytes;

        try {
            bytes = Files.readAllBytes(file.toPath());
            String eTag = calculateETag(bytes);
            checkSelectors(request, eTag, file);
            return S3Object.build()
                    .setAbsolutePath(absolutePath.get())
                    .setKey(request.getKey())
                    .setETag(eTag)
                    .setFile(file)
                    .setRawBytes(bytes)
                    .setLastModified(DateTimeUtil.parseDateTime(file))
                    .setMetaData(metadataDriver.getObjectMetadata(request.getBucket(), request.getKey()));
        } catch (IOException | ParseException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't create object: " + absolutePath)
                    .setMessage("Internal error : can't create object")
                    .setResource(File.separatorChar + request.getBucket() + request.getKey())
                    .setRequestId("1");
        }
    }

    private void checkSelectors(ParsedRequest request, String eTag, File file) throws ParseException {
        //TODO
        if (request.containsHeader(S3Headers.IF_MATCH)) {
            strSelectors.get(S3Headers.IF_MATCH).check(eTag, request.getHeader(S3Headers.IF_MATCH));
        }
        if (request.containsHeader(S3Headers.IF_MODIFIED_SINCE)) {
            dateSelectors.get(S3Headers.IF_MODIFIED_SINCE).check(new Date(file.lastModified()),
                    DateTimeUtil.parseStrTime(request.getHeader(S3Headers.IF_MODIFIED_SINCE))); //TODO
        }
        if (request.containsHeader(S3Headers.IF_NONE_MATCH)) {
            strSelectors.get(S3Headers.IF_NONE_MATCH).check(eTag, request.getHeader(S3Headers.IF_NONE_MATCH));
        }
        if (request.containsHeader(S3Headers.IF_UNMODIFIED_SINCE)) {
            dateSelectors.get(S3Headers.IF_UNMODIFIED_SINCE).check(new Date(file.lastModified()),
                    DateTimeUtil.parseStrTime(request.getHeader(S3Headers.IF_UNMODIFIED_SINCE))); // TODO
        }
    }

    @Override
    public S3Object putObject(String bucket, String key, String basePath, byte[] bytes, Map<String, String> metadata,
                              S3User s3User) throws S3Exception {
        Optional<String> absolutePath = buildPath(basePath, bucket, key);
        if (absolutePath.isEmpty()) {
            //TODO
            return null;
        }
        File file = new File(absolutePath.get());
        if (!processFolders(file, bucket)) {
            throw S3Exception.INTERNAL_ERROR("Can't create folders: " + absolutePath)
                    .setMessage("Internal error : can't create folder")
                    .setResource(File.separatorChar + bucket + key)
                    .setRequestId("1");
        }
        try {
            log.info("Starting creating file : " + file.getAbsolutePath() + " # " + file.getPath());
            if (file.createNewFile() || file.exists()) {
                FileOutputStream outputStream = new FileOutputStream(file);
                outputStream.write(bytes);
                outputStream.close();
                metadataDriver.setObjectMetadata(bucket, key, metadata);
                putObjectAcl(bucket, key, createDefaultAccessControlPolicy(s3User));
                return S3Object.build()
                        .setAbsolutePath(absolutePath.get())
                        .setKey(key)
                        .setETag(calculateETag(bytes))
                        .setFile(file)
                        .setRawBytes(bytes)
                        .setLastModified(DateTimeUtil.parseDateTime(file))
                        .setMetaData(metadata);
            } else {
                throw S3Exception.INTERNAL_ERROR("Can't create object: " + absolutePath)
                        .setMessage("Internal error : can't create object")
                        .setResource(File.separatorChar + bucket + key)
                        .setRequestId("1");
            }
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't create object: " + absolutePath)
                    .setMessage(exception.getMessage())
                    .setResource(File.separatorChar + bucket + key)
                    .setRequestId("1");
        }
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

    //TODO
    private Optional<String> buildPath(String basePath, String bucket, String key) {
        if (key == null || bucket == null) {
            return Optional.empty();
        }
        return Optional.of(basePath + File.separatorChar + bucket + key);
    }

    private boolean processFolders(File file, String bucket) {
        File folder = file.getParentFile();
        if (folder.exists() && folder.isDirectory()) {
            return true;
        } else if (folder.exists() && !folder.isDirectory()) {
            return false;
        } else if (!folder.getName().equals(bucket)) {
            return processFolders(folder, bucket) && folder.mkdir();
        }
        return true;
    }

    private String calculateETag(byte[] bytes) {
        return DigestUtils.md5Hex(bytes);
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
