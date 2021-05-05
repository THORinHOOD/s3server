package com.thorinhood.drivers.entity;

import com.thorinhood.data.*;
import com.thorinhood.data.s3object.HasMetaData;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.drivers.FileDriver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.selectors.*;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.Pair;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    public void createBucket(S3BucketPath s3BucketPath, S3User s3User) throws S3Exception {
        String absolutePath = s3BucketPath.getFullPathToBucket(BASE_FOLDER_PATH);
        File bucketFile = new File(absolutePath);
        if (bucketFile.exists()) {
            throw S3Exception.build("Bucket already exists : " + absolutePath)
                    .setStatus(HttpResponseStatus.CONFLICT)
                    .setCode(S3ResponseErrorCodes.BUCKET_ALREADY_OWNED_BY_YOU)
                    .setMessage("Your previous request to create the named bucket succeeded and you already own it.")
                    .setResource(File.separatorChar + s3BucketPath.getBucket())
                    .setRequestId("1"); // TODO
        }
        if (!bucketFile.mkdir()) {
            throw S3Exception.INTERNAL_ERROR("Can't create bucket: " + absolutePath)
                    .setMessage("Can't create bucket")
                    .setResource(File.separatorChar + s3BucketPath.getBucket())
                    .setRequestId("1"); // TODO
        }
    }

    @Override
    public HasMetaData getObject(S3ObjectPath s3ObjectPath, HttpHeaders httpHeaders) throws S3Exception {
        String absolutePath = s3ObjectPath.getFullPathToObject(BASE_FOLDER_PATH);
        File file = new File(absolutePath);
        if (file.isHidden() || !file.exists() || !file.isFile()) {
            throw S3Exception.build("File not found: " + absolutePath)
                    .setStatus(HttpResponseStatus.NOT_FOUND)
                    .setCode(S3ResponseErrorCodes.NO_SUCH_KEY)
                    .setMessage("The resource you requested does not exist")
                    .setResource(File.separatorChar + s3ObjectPath.getKeyWithBucket())
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
                    .setS3Path(s3ObjectPath)
                    .setETag(eTag)
                    .setFile(file)
                    .setRawBytes(bytes)
                    .setLastModified(DateTimeUtil.parseDateTime(file));
        } catch (IOException | ParseException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't create object: " + absolutePath)
                    .setMessage("Internal error : can't create object")
                    .setResource(File.separatorChar + s3ObjectPath.getKeyWithBucket())
                    .setRequestId("1"); // TODO
        }
    }

    @Override
    public S3Object putObject(S3ObjectPath s3ObjectPath, byte[] bytes, Map<String, String> metadata) throws S3Exception {
        String absolutePath = s3ObjectPath.getFullPathToObject(BASE_FOLDER_PATH);
        File file = new File(absolutePath);
        if (!processFolders(file, s3ObjectPath.getBucket())) {
            throw S3Exception.INTERNAL_ERROR("Can't create folders: " + absolutePath)
                    .setMessage("Internal error : can't create folder")
                    .setResource(File.separatorChar + s3ObjectPath.getKeyWithBucket())
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
                        .setS3Path(s3ObjectPath)
                        .setETag(calculateETag(bytes))
                        .setFile(file)
                        .setRawBytes(bytes)
                        .setLastModified(DateTimeUtil.parseDateTime(file))
                        .setMetaData(metadata);
            } else {
                throw S3Exception.INTERNAL_ERROR("Can't create object: " + absolutePath)
                        .setMessage("Internal error : can't create object")
                        .setResource(File.separatorChar + s3ObjectPath.getKeyWithBucket())
                        .setRequestId("1"); // TODO
            }
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't create object: " + absolutePath)
                    .setMessage(exception.getMessage())
                    .setResource(File.separatorChar + s3ObjectPath.getKeyWithBucket())
                    .setRequestId("1"); // TODO
        }
    }

    @Override
    public void deleteObject(S3ObjectPath s3ObjectPath) throws S3Exception {
        String pathToObject = s3ObjectPath.getFullPathToObject(BASE_FOLDER_PATH);
        String pathToObjectMetadataFolder = getPathToObjectMetadataFolder(s3ObjectPath, false);
        deleteFolder(pathToObjectMetadataFolder);
        deleteFile(pathToObject);
    }

    @Override
    public void deleteBucket(S3BucketPath s3BucketPath) throws S3Exception {
        String pathToBucket = s3BucketPath.getFullPathToBucket(BASE_FOLDER_PATH);
        String pathToBucketMetadataFolder = getPathToBucketMetadataFolder(s3BucketPath, false);
        deleteFolder(pathToBucket);
        deleteFolder(pathToBucketMetadataFolder);
    }

    @Override
    public Pair<Pair<List<HasMetaData>, Boolean>, String> getBucketObjects(GetBucketObjects getBucketObjects) throws S3Exception {
        Path path = Path.of(BASE_FOLDER_PATH + File.separatorChar + getBucketObjects.getBucket());
        List<HasMetaData> objects = new ArrayList<>();
        boolean truncated = false;
        boolean start = false;
        String nextContinuationToken = null;
        try {
            try (Stream<Path> tree = Files.walk(path)) {
                int from = path.toString().length() + 1;
                List<String> allObjects = tree.filter(current -> !isMetadataFolder(current) &&
                        !isMetadataFile(current) && !Files.isDirectory(current))
                        .map(objectPath -> objectPath.toString().substring(from))
                        .sorted(String::compareTo)
                        .collect(Collectors.toList());
                for (int i = 0; (i < allObjects.size()) && !truncated; i++) {
                    String currentPath = allObjects.get(i);
                    String objectMd5 = DigestUtils.md5Hex(currentPath);
                    if (objects.size() == getBucketObjects.getMaxKeys()) {
                        truncated = true;
                        nextContinuationToken = objectMd5;
                    } else {
                        if (getBucketObjects.getContinuationToken() != null) {
                            if (objectMd5.equals(getBucketObjects.getContinuationToken())) {
                                start = true;
                            }
                        } else {
                            start = true;
                        }
                        if (start) {
                            if (getBucketObjects.getStartAfter() == null ||
                                    getBucketObjects.getStartAfter().compareTo(currentPath) <= 0) {
                                if (checkPrefix(getBucketObjects.getPrefix(), currentPath)) {
                                    objects.add(getObject(S3ObjectPath.raw(getBucketObjects.getBucket(), currentPath),
                                            null));
                                }
                            }
                        }
                    }
                }
            }
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't list bucket objects")
                    .setMessage("Can't list bucket objects")
                    .setResource("1")
                    .setRequestId("1");
        }
        return Pair.of(Pair.of(objects, truncated), nextContinuationToken);
    }

    private boolean checkPrefix(String prefix, String path) {
        if (prefix == null) {
            return true;
        }
        return path.startsWith(prefix);
    }

    @Override
    public boolean isBucketExists(S3BucketPath s3BucketPath) throws S3Exception {
        File bucketFile = new File(s3BucketPath.getFullPathToBucket(BASE_FOLDER_PATH));
        File metadataFolder = new File(getPathToBucketMetadataFolder(s3BucketPath, false));
        return bucketFile.exists() && bucketFile.isDirectory() && metadataFolder.exists() &&
                metadataFolder.isDirectory();
    }

    @Override
    public boolean isObjectExists(S3ObjectPath s3ObjectPath) throws S3Exception {
        File file = new File(s3ObjectPath.getFullPathToObject(BASE_FOLDER_PATH));
        return file.exists() && file.isFile();
    }

    @Override
    public List<Pair<String, String>> getBuckets(S3User s3User) throws S3Exception {
        Path path = Path.of(BASE_FOLDER_PATH);
        List<Pair<String, String>> buckets;
        try {
            try (Stream<Path> tree = Files.walk(path, 1)) {
                buckets = tree.filter(entity -> !isMetadataFolder(entity) && !isConfigFolder(entity) &&
                        Files.isDirectory(entity) && !entity.equals(path) &&
                        isBucketExists(S3BucketPath.build(entity.getFileName().toString())))
                    .map(entity -> {
                        File bucket = entity.toFile();
                        try {
                            BasicFileAttributes attr = Files.readAttributes(entity, BasicFileAttributes.class);
                            return Pair.of(bucket.getName(),
                                    DateTimeUtil.parseDateTimeISO(attr.creationTime().toMillis()));
                        } catch (IOException e) {
                            throw S3Exception.INTERNAL_ERROR("Can't get bucket attributes :" + entity.getFileName())
                                    .setMessage("Can't get bucket attributes : " + entity.getFileName())
                                    .setResource("1")
                                    .setRequestId("1"); // TODO
                        }
                    }).collect(Collectors.toList());
            }
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't list bucket objects")
                    .setMessage("Can't list bucket objects")
                    .setResource("1")
                    .setRequestId("1");
        }
        return buckets;
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
