package com.thorinhood.utils;

import com.thorinhood.data.S3Headers;
import com.thorinhood.data.S3Object;
import com.thorinhood.data.S3ResponseErrorCodes;
import com.thorinhood.db.H2DB;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.selectors.*;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

public class S3Util {

    private final H2DB h2DB;
    private final Map<String, Selector<String>> strSelectors;
    private final Map<String, Selector<Date>> dateSelectors;

    public S3Util(H2DB h2DB) {
        this.h2DB = h2DB;
        strSelectors = Map.of(
                S3Headers.IF_MATCH, new IfMatch(),
                S3Headers.IF_NONE_MATCH, new IfNoneMatch()
        );
        dateSelectors = Map.of(
                S3Headers.IF_MODIFIED_SINCE, new IfModifiedSince(),
                S3Headers.IF_UNMODIFIED_SINCE, new IfUnmodifiedSince()
        );
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
    }

    public S3Object getObject(ParsedRequest request, String basePath) throws S3Exception {
        Optional<String> absolutePath = buildPath(request.getBucket(), request.getKey(), basePath);
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
            //TODO
            if (request.getIfMatchHeader() != null) {
                strSelectors.get(S3Headers.IF_MATCH).check(eTag, request.getIfMatchHeader());
            }
            if (request.getIfModifiedHeader() != null) {
                dateSelectors.get(S3Headers.IF_MODIFIED_SINCE).check(new Date(file.lastModified()),
                        DateTimeUtil.parseStrTime(request.getIfModifiedHeader())); //TODO
            }
            if (request.getIfNoneMatchHeader() != null) {
                strSelectors.get(S3Headers.IF_NONE_MATCH).check(eTag, request.getIfNoneMatchHeader());
            }
            if (request.getIfUnmodifiedSince() != null) {
                dateSelectors.get(S3Headers.IF_UNMODIFIED_SINCE).check(new Date(file.lastModified()),
                        DateTimeUtil.parseStrTime(request.getIfModifiedHeader())); // TODO
            }

            return S3Object.build()
                    .setAbsolutePath(absolutePath.get())
                    .setKey(request.getKey())
                    .setETag(eTag)
                    .setFile(file)
                    .setRawBytes(bytes)
                    .setLastModified(DateTimeUtil.parseDateTime(file))
                    .setMetaData(h2DB.getFileMetadata(absolutePath.get()));
        } catch (IOException | SQLException | ParseException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't create object: " + absolutePath)
                    .setMessage("Internal error : can't create object")
                    .setResource(File.separatorChar + request.getBucket() + request.getKey())
                    .setRequestId("1");
        }
    }

    public S3Object putObject(String bucket, String key, String basePath, byte[] bytes, Map<String, String> metadata)
            throws S3Exception {
        Optional<String> absolutePath = buildPath(bucket, key, basePath);
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
            if (file.createNewFile() || file.exists()) {
                FileOutputStream outputStream = new FileOutputStream(file);
                outputStream.write(bytes);
                outputStream.close();
                h2DB.setFileMetadata(absolutePath.get(), metadata);
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
        } catch (IOException | SQLException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't create object: " + absolutePath)
                    .setMessage("Internal error : can't create object")
                    .setResource(File.separatorChar + bucket + key)
                    .setRequestId("1");
        }
    }

    //TODO
    private Optional<String> buildPath(String bucket, String key, String basePath) {
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
