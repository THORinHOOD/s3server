package com.thorinhood.drivers.entity;

import com.thorinhood.data.S3Headers;
import com.thorinhood.data.S3ResponseErrorCodes;
import com.thorinhood.data.S3User;
import com.thorinhood.data.s3object.HasMetaData;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.drivers.FileDriver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.selectors.*;
import com.thorinhood.utils.DateTimeUtil;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class FileEntityDriver extends FileDriver implements EntityDriver {

    private static final Logger log = LogManager.getLogger(FileEntityDriver.class);

    private final Map<String, Selector<String>> strSelectors;
    private final Map<String, Selector<Date>> dateSelectors;

    public FileEntityDriver(String baseFolderPath, String configFolderPath, String usersFolderPath) {
        super(baseFolderPath, configFolderPath, usersFolderPath);
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
    public void createBucket(String bucket, S3User s3User) throws S3Exception {
        String absolutePath = BASE_FOLDER_PATH + File.separatorChar + bucket;
        File bucketFile = new File(absolutePath);
        if (bucketFile.exists()) {
            throw S3Exception.build("Bucket already exists : " + absolutePath)
                    .setStatus(HttpResponseStatus.CONFLICT)
                    .setCode(S3ResponseErrorCodes.BUCKET_ALREADY_OWNED_BY_YOU)
                    .setMessage("Your previous request to create the named bucket succeeded and you already own it.")
                    .setResource(File.separatorChar + bucket)
                    .setRequestId("1"); // TODO
        }
        if (!bucketFile.mkdir()) {
            throw S3Exception.INTERNAL_ERROR("Can't create bucket: " + absolutePath)
                    .setMessage("Can't create bucket")
                    .setResource(File.separatorChar + bucket)
                    .setRequestId("1"); // TODO
        }
    }

    @Override
    public HasMetaData getObject(String bucket, String key, HttpHeaders httpHeaders) throws S3Exception {
        String absolutePath = fullPath(bucket, key);
        File file = new File(absolutePath);
        if (file.isHidden() || !file.exists() || !file.isFile()) {
            throw S3Exception.build("File not found: " + absolutePath)
                    .setStatus(HttpResponseStatus.NOT_FOUND)
                    .setCode(S3ResponseErrorCodes.NO_SUCH_KEY)
                    .setMessage("The resource you requested does not exist")
                    .setResource(File.separatorChar + bucket + key)
                    .setRequestId("1"); // TODO
        }

        byte[] bytes;
        try {
            bytes = Files.readAllBytes(file.toPath());
            String eTag = calculateETag(bytes);
            if (httpHeaders != null) {
                checkSelectors(httpHeaders, eTag, file);
            }
            return S3Object.build()
                    .setAbsolutePath(absolutePath)
                    .setKey(key)
                    .setETag(eTag)
                    .setFile(file)
                    .setRawBytes(bytes)
                    .setLastModified(DateTimeUtil.parseDateTime(file));
        } catch (IOException | ParseException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't create object: " + absolutePath)
                    .setMessage("Internal error : can't create object")
                    .setResource(File.separatorChar + bucket + key)
                    .setRequestId("1"); // TODO
        }
    }

    @Override
    public S3Object putObject(String bucket, String key, byte[] bytes, Map<String, String> metadata) throws S3Exception {
        String absolutePath = fullPath(bucket, key);
        File file = new File(absolutePath);
        if (!processFolders(file, bucket)) {
            throw S3Exception.INTERNAL_ERROR("Can't create folders: " + absolutePath)
                    .setMessage("Internal error : can't create folder")
                    .setResource(File.separatorChar + bucket + key)
                    .setRequestId("1"); // TODO
        }
        try {
            log.info("Starting creating file : " + file.getAbsolutePath() + " # " + file.getPath());
            if (file.createNewFile() || file.exists()) {
                FileOutputStream outputStream = new FileOutputStream(file);
                outputStream.write(bytes);
                outputStream.close();
                return S3Object.build()
                        .setAbsolutePath(absolutePath)
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
                        .setRequestId("1"); // TODO
            }
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't create object: " + absolutePath)
                    .setMessage(exception.getMessage())
                    .setResource(File.separatorChar + bucket + key)
                    .setRequestId("1"); // TODO
        }
    }

    @Override
    public void deleteObject(String bucket, String key) throws S3Exception {
        String pathToObject = fullPath(bucket, key);
        String pathToObjectMetadataFolder = getPathToObjectMetadataFolder(bucket, key, false);
        deleteFolder(pathToObjectMetadataFolder);
        deleteFile(pathToObject);
    }

    @Override
    public List<HasMetaData> getBucketObjects(String bucket) throws S3Exception {
        Path path = Path.of(BASE_FOLDER_PATH + File.separatorChar + bucket);
        List<HasMetaData> objects = new ArrayList<>();
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    if (dir.getFileName().toString().startsWith(".#")) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    return FileVisitResult.CONTINUE;
                }
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    String path = file.toString();
                    String key = path.substring(path.indexOf(bucket) + bucket.length());
                    objects.add(getObject(bucket, key, null));
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't list bucket objects")
                    .setMessage("Can't list bucket objects")
                    .setResource("1")
                    .setRequestId("1");
        }
        return objects;
    }

    @Override
    public boolean isBucketExists(String bucket) throws S3Exception {
        String absolutePath = BASE_FOLDER_PATH + File.separatorChar + bucket;
        File bucketFile = new File(absolutePath);
        return bucketFile.exists() && bucketFile.isDirectory();
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

    private void checkSelectors(HttpHeaders headers, String eTag, File file) throws ParseException {
        //TODO
        if (headers.contains(S3Headers.IF_MATCH)) {
            strSelectors.get(S3Headers.IF_MATCH).check(eTag, headers.get(S3Headers.IF_MATCH));
        }
        if (headers.contains(S3Headers.IF_MODIFIED_SINCE)) {
            dateSelectors.get(S3Headers.IF_MODIFIED_SINCE).check(new Date(file.lastModified()),
                    DateTimeUtil.parseStrTime(headers.get(S3Headers.IF_MODIFIED_SINCE))); //TODO
        }
        if (headers.contains(S3Headers.IF_NONE_MATCH)) {
            strSelectors.get(S3Headers.IF_NONE_MATCH).check(eTag, headers.get(S3Headers.IF_NONE_MATCH));
        }
        if (headers.contains(S3Headers.IF_UNMODIFIED_SINCE)) {
            dateSelectors.get(S3Headers.IF_UNMODIFIED_SINCE).check(new Date(file.lastModified()),
                    DateTimeUtil.parseStrTime(headers.get(S3Headers.IF_UNMODIFIED_SINCE))); // TODO
        }
    }
}
