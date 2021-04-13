package com.thorinhood.utils;

import com.thorinhood.acl.*;
import com.thorinhood.data.S3Headers;
import com.thorinhood.data.S3Object;
import com.thorinhood.data.S3ResponseErrorCodes;
import com.thorinhood.db.AclDriver;
import com.thorinhood.db.MetadataDriver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.selectors.*;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

public class S3Util {

    private final MetadataDriver metadataDriver;
    private final AclDriver aclDriver;
    private final Map<String, Selector<String>> strSelectors;
    private final Map<String, Selector<Date>> dateSelectors;
    private final AccessControlPolicy defaultOwnerAcl = AccessControlPolicy.builder()    // TODO
            .setOwner(Owner.builder()
                    .setId("1")
                    .setDisplayName("asgar")
                    .build())
            .setAccessControlList(Collections.singletonList(
                    Grant.builder()
                            .setGrantee(Grantee.builder()
                                    .setDisplayName("asgar")
                                    .setId("1")
                                    .setType("Canonical User")
                                    .build())
                            .setPermission(Permission.FULL_CONTROL)
                            .build()
            ))
            .build();


    private static final Logger log = LogManager.getLogger(S3Util.class);

    public S3Util(MetadataDriver metadataDriver, AclDriver aclDriver) {
        this.metadataDriver = metadataDriver;
        this.aclDriver = aclDriver;
        strSelectors = Map.of(
                S3Headers.IF_MATCH, new IfMatch(),
                S3Headers.IF_NONE_MATCH, new IfNoneMatch()
        );
        dateSelectors = Map.of(
                S3Headers.IF_MODIFIED_SINCE, new IfModifiedSince(),
                S3Headers.IF_UNMODIFIED_SINCE, new IfUnmodifiedSince()
        );
    }

    public AccessControlPolicy getBucketAcl(String basePath, String bucket) throws S3Exception {
        return aclDriver.getBucketAcl(basePath, bucket);
    }

    public AccessControlPolicy getObjectAcl(String basePath, String bucket, String key) throws S3Exception {
        Optional<String> path = buildPath(basePath, bucket, key);
        if (path.isEmpty()) {
            //TODO
            throw S3Exception.INTERNAL_ERROR("Can't build path to object")
                    .setMessage("Can't build path to object")
                    .setResource("1")
                    .setRequestId("1");
        }
        return aclDriver.getObjectAcl(path.get());
    }

    public void putBucketAcl(String basePath, String bucket, byte[] bytes) throws S3Exception {
        putBucketAcl(basePath, bucket, aclDriver.parseFromBytes(bytes));
    }

    public void putBucketAcl(String basePath, String bucket, AccessControlPolicy acl) throws S3Exception {
        aclDriver.putBucketAcl(basePath, bucket, acl);
    }

    public String putObjectAcl(String basePath, String bucket, String key, byte[] bytes) throws S3Exception {
        return putObjectAcl(basePath, bucket, key, aclDriver.parseFromBytes(bytes));
    }

    public String putObjectAcl(String basePath, String bucket, String key, AccessControlPolicy acl) throws S3Exception {
        Optional<String> path = buildPath(basePath, bucket, key);
        if (path.isEmpty()) {
            //TODO
            throw S3Exception.INTERNAL_ERROR("Can't build path to object")
                    .setMessage("Can't build path to object")
                    .setResource("1")
                    .setRequestId("1");
        }
        return aclDriver.putObjectAcl(path.get(), acl);
    }

    public void createBucket(String bucket, String basePath) throws S3Exception {
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
        putBucketAcl(basePath, bucket, defaultOwnerAcl);
    }

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
                    .setMetaData(metadataDriver.getObjectMetadata(absolutePath.get()));
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

    public S3Object putObject(String bucket, String key, String basePath, byte[] bytes, Map<String, String> metadata)
            throws S3Exception {
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
                metadataDriver.setObjectMetadata(absolutePath.get(), metadata);
                putObjectAcl(basePath, bucket, key, defaultOwnerAcl);
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

}
