package com.thorinhood.drivers.entity;

import com.thorinhood.data.list.raw.ListBucketResultRaw;
import com.thorinhood.data.list.request.GetBucketObjects;
import com.thorinhood.data.list.request.GetBucketObjectsV2;
import com.thorinhood.data.S3FileBucketPath;
import com.thorinhood.data.S3FileObjectPath;
import com.thorinhood.data.S3User;
import com.thorinhood.data.multipart.Part;
import com.thorinhood.data.requests.S3Headers;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import com.thorinhood.data.list.raw.ListBucketV2ResultRaw;
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

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileEntityDriver extends FileDriver implements EntityDriver {

    private static final Logger log = LogManager.getLogger(FileEntityDriver.class);

    private static final String ENTITY_TOO_SMALL = "Your proposed upload is smaller than the minimum allowed " +
            "object size. Each part must be at least 5 MB in size.";
    private static final long MIN_PART_SIZE = 5242880L;

    private final Selector<String> ifMatch;
    private final Selector<String> ifNoneMatch;
    private final Selector<Date> ifModifiedSince;
    private final Selector<Date> ifUnmodifiedSince;

    public FileEntityDriver(String baseFolderPath, String configFolderPath, String usersFolderPath) {
        super(baseFolderPath, configFolderPath, usersFolderPath);
        ifMatch = new IfMatch();
        ifNoneMatch = new IfNoneMatch();
        ifModifiedSince = new IfModifiedSince();
        ifUnmodifiedSince = new IfUnmodifiedSince();
    }

    @Override
    public void createBucket(S3FileBucketPath s3FileBucketPath, S3User s3User) throws S3Exception {
        String absolutePath = s3FileBucketPath.getPathToBucket();
        File bucketFile = new File(absolutePath);
        if (bucketFile.exists() || !bucketFile.mkdir()) {
            throw S3Exception.INTERNAL_ERROR("Can't create bucket: " + absolutePath);
        }
    }

    @Override
    public HasMetaData getObject(S3FileObjectPath s3FileObjectPath, String eTag, HttpHeaders httpHeaders,
                                 boolean isCopyRead) throws S3Exception {
        String absolutePath = s3FileObjectPath.getPathToObject();
        File file = new File(absolutePath);
        if (file.isHidden() || !file.exists() || !file.isFile()) {
            throw S3Exception.builder("File not found: " + absolutePath)
                    .setStatus(HttpResponseStatus.NOT_FOUND)
                    .setCode(S3ResponseErrorCodes.NO_SUCH_KEY)
                    .setMessage("The resource you requested does not exist")
                    .build();
        }
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(file.toPath());
            if (httpHeaders != null && eTag != null) {
                if (isCopyRead) {
                    checkSelectorsCopy(httpHeaders, eTag, file);
                } else {
                    checkSelectors(httpHeaders, eTag, file);
                }
            }
            return S3Object.build()
                    .setAbsolutePath(absolutePath)
                    .setS3Path(s3FileObjectPath)
                    .setETag(eTag)
                    .setFile(file)
                    .setRawBytes(bytes)
                    .setLastModified(DateTimeUtil.parseDateTime(file));
        } catch (ParseException | IOException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't create object: " + absolutePath);
        }
    }

    @Override
    public HasMetaData headObject(S3FileObjectPath s3FileObjectPath, String eTag, HttpHeaders httpHeaders)
            throws S3Exception {
        String absolutePath = s3FileObjectPath.getPathToObject();
        File file = new File(absolutePath);
        if (file.isHidden() || !file.exists() || !file.isFile()) {
            throw S3Exception.builder("File not found: " + absolutePath)
                    .setStatus(HttpResponseStatus.NOT_FOUND)
                    .setCode(S3ResponseErrorCodes.NO_SUCH_KEY)
                    .setMessage("The resource you requested does not exist")
                    .build();
        }
        try {
            if (httpHeaders != null && eTag != null) {
                checkSelectors(httpHeaders, eTag, file);
            }
            return S3Object.build()
                    .setAbsolutePath(absolutePath)
                    .setS3Path(s3FileObjectPath)
                    .setETag(eTag)
                    .setFile(file)
                    .setRawBytes(null)
                    .setLastModified(DateTimeUtil.parseDateTime(file));
        } catch (ParseException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't create object: " + absolutePath);
        }
    }

    @Override
    public S3Object putObject(S3FileObjectPath s3FileObjectPath, byte[] bytes, Map<String, String> metadata)
            throws S3Exception {
        String absolutePath = s3FileObjectPath.getPathToObject();
        File file = new File(absolutePath);
        Path objectMetadataFolder = new File(s3FileObjectPath.getPathToObjectMetadataFolder()).toPath();
        Path source = createPreparedTmpFile(objectMetadataFolder, file.toPath(), bytes);
        commitFile(source, file.toPath());
        return S3Object.build()
                .setAbsolutePath(absolutePath)
                .setS3Path(s3FileObjectPath)
                .setETag(calculateETag(bytes))
                .setFile(file)
                .setRawBytes(bytes)
                .setLastModified(DateTimeUtil.parseDateTime(file))
                .setMetaData(metadata);
    }

    @Override
    public void deleteObject(S3FileObjectPath s3FileObjectPath) throws S3Exception {
        String pathToObject = s3FileObjectPath.getPathToObject();
        deleteFile(pathToObject);
        deleteFolder(s3FileObjectPath.getPathToObjectMetadataFolder());
        deleteEmptyKeys(new File(pathToObject));
    }

    @Override
    public void deleteBucket(S3FileBucketPath s3FileBucketPath) throws S3Exception {
        String pathToBucket = s3FileBucketPath.getPathToBucket();
        String pathToBucketMetadataFolder = s3FileBucketPath.getPathToBucketMetadataFolder();
        deleteFolder(pathToBucket);
        deleteFolder(pathToBucketMetadataFolder);
    }

    private String commonPrefix(String key, String prefix, String delimiter) {
        if (delimiter == null || delimiter.equals("")) {
            return null;
        }
        int index = key.indexOf(delimiter, prefix.length());
        if (index == -1) {
            return null;
        }
        return key.substring(0, index + delimiter.length());
    }

    @Override
    public ListBucketV2ResultRaw getBucketObjectsV2(GetBucketObjectsV2 getBucketObjectsV2) throws S3Exception {
        Path path = Path.of(BASE_FOLDER_PATH + File.separatorChar + getBucketObjectsV2.getBucket());
        List<S3FileObjectPath> objects = new ArrayList<>();
        Set<String> commonPrefixes = new TreeSet<>();
        int keyCount = 0;
        boolean truncated = false;
        boolean start = false;
        String nextContinuationToken = null;
        try (Stream<Path> tree = Files.walk(path)) {
            List<String> allObjects = getObjectsKeys(tree, path.toString().length() + 1);
            for (int i = 0; (i < allObjects.size()) && !truncated; i++) {
                String currentPath = allObjects.get(i);
                String objectMd5 = DigestUtils.md5Hex(currentPath);
                if (objects.size() == getBucketObjectsV2.getMaxKeys()) {
                    truncated = true;
                    nextContinuationToken = objectMd5;
                } else {
                    if (getBucketObjectsV2.getContinuationToken() != null) {
                        if (objectMd5.equals(getBucketObjectsV2.getContinuationToken())) {
                            start = true;
                        }
                    } else {
                        start = true;
                    }
                    if (start) {
                        if (getBucketObjectsV2.getStartAfter() == null || getBucketObjectsV2.getStartAfter()
                                .compareTo(currentPath) <= 0) {
                            if (checkPrefix(getBucketObjectsV2.getPrefix(), currentPath)) {
                                String commonPrefix = commonPrefix(currentPath, getBucketObjectsV2.getPrefix(),
                                        getBucketObjectsV2.getDelimiter());
                                if (commonPrefix != null) {
                                    commonPrefixes.add(commonPrefix);
                                } else {
                                    objects.add(S3FileObjectPath.raw(BASE_FOLDER_PATH, getBucketObjectsV2.getBucket(),
                                            currentPath));
                                }
                                keyCount++;
                            }
                        }
                    }
                }
            }
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception);
        }
        return ListBucketV2ResultRaw.builder()
                .setIsTruncated(truncated)
                .setNextContinuationToken(nextContinuationToken)
                .setS3FileObjectsPaths(objects)
                .setCommonPrefixes(commonPrefixes)
                .setKeyCount(keyCount)
                .build();
    }

    private List<String> getObjectsKeys(Stream<Path> tree, int getKeyFrom) {
        return tree.filter(current -> !isMetadataFolder(current) &&
                !isMetadataFile(current) && !Files.isDirectory(current))
                .map(objectPath -> objectPath.toString().substring(getKeyFrom))
                .sorted(String::compareTo)
                .collect(Collectors.toList());
    }

    @Override
    public ListBucketResultRaw getBucketObjects(GetBucketObjects getBucketObjects) throws S3Exception {
        Path path = Path.of(BASE_FOLDER_PATH + File.separatorChar + getBucketObjects.getBucket());
        List<S3FileObjectPath> objects = new ArrayList<>();
        Set<String> commonPrefixes = new TreeSet<>();
        boolean truncated = false;
        String nextMarker = null;
        try (Stream<Path> tree = Files.walk(path)) {
            List<String> allObjects = getObjectsKeys(tree, path.toString().length() + 1);
            for (int i = 0; (i < allObjects.size()) && !truncated; i++) {
                String currentPath = allObjects.get(i);
                if (objects.size() == getBucketObjects.getMaxKeys()) {
                    truncated = true;
                } else {
                    if (getBucketObjects.getMarker() == null || getBucketObjects.getMarker()
                            .compareTo(currentPath) <= 0) {
                        if (checkPrefix(getBucketObjects.getPrefix(), currentPath)) {
                            String commonPrefix = commonPrefix(currentPath, getBucketObjects.getPrefix(),
                                    getBucketObjects.getDelimiter());
                            if (commonPrefix != null) {
                                commonPrefixes.add(commonPrefix);
                                nextMarker = commonPrefix;
                            } else {
                                objects.add(S3FileObjectPath.raw(BASE_FOLDER_PATH, getBucketObjects.getBucket(),
                                        currentPath));
                                nextMarker = currentPath;
                            }
                        }
                    }
                }
            }
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception);
        }
        return ListBucketResultRaw.builder()
                .setIsTruncated(truncated)
                .setS3FileObjectsPaths(objects)
                .setCommonPrefixes(commonPrefixes)
                .setNextMarker(nextMarker)
                .build();
    }

    @Override
    public List<Pair<S3FileBucketPath, String>> getBuckets(S3User s3User) throws S3Exception {
        Path path = Path.of(BASE_FOLDER_PATH);
        List<Pair<S3FileBucketPath, String>> buckets;
        try {
            try (Stream<Path> tree = Files.walk(path, 1)) {
                buckets = tree.filter(entity -> isFolderExists(entity) && !isMetadataFolder(entity) &&
                                                !isConfigFolder(entity) && isBucket(entity) && !entity.equals(path))
                    .map(entity -> {
                        File bucket = entity.toFile();
                        try {
                            BasicFileAttributes attr = Files.readAttributes(entity, BasicFileAttributes.class);
                            return Pair.of(new S3FileBucketPath(BASE_FOLDER_PATH, bucket.getName()),
                                    DateTimeUtil.parseDateTimeISO(attr.creationTime().toMillis()));
                        } catch (IOException e) {
                            throw S3Exception.INTERNAL_ERROR("Can't get bucket attributes :" + entity.getFileName());
                        }
                    }).collect(Collectors.toList());
            }
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't list bucket objects");
        }
        return buckets;
    }

    @Override
    public void createMultipartUpload(S3FileObjectPath s3FileObjectPath, String uploadId) throws S3Exception {
        String multipartFolder = s3FileObjectPath.getPathToObjectMultipartFolder();
        createFolder(multipartFolder);
        String currentUploadFolder = s3FileObjectPath.getPathToObjectUploadFolder(uploadId);
        createFolder(currentUploadFolder);
    }

    @Override
    public void abortMultipartUpload(S3FileObjectPath s3FileObjectPath, String uploadId) throws S3Exception {
        String multipartFolder = s3FileObjectPath.getPathToObjectMultipartFolder();
        if (!isFolderExists(multipartFolder)) {
            return;
        }
        String uploadFolder = s3FileObjectPath.getPathToObjectUploadFolder(uploadId);
        if (!isFolderExists(uploadFolder)) {
            return;
        }
        deleteFolder(uploadFolder);
        deleteEmptyKeys(new File(uploadFolder));
    }

    @Override
    public String putUploadPart(S3FileObjectPath s3FileObjectPath, String uploadId, int partNumber, byte[] bytes)
            throws S3Exception {
        String currentUploadFolder = s3FileObjectPath.getPathToObjectUploadFolder(uploadId);
        if (!isFolderExists(currentUploadFolder)) {
            throw S3Exception.NO_SUCH_UPLOAD(uploadId);
        }
        String partPathStr = s3FileObjectPath.getPathToObjectUploadPart(uploadId, partNumber);
        Path partPath = new File(partPathStr).toPath();
        Path source = createPreparedTmpFile(new File(currentUploadFolder).toPath(), partPath, bytes);
        commitFile(source, partPath);
        return calculateETag(bytes);
    }

    @Override
    public String completeMultipartUpload(S3FileObjectPath s3FileObjectPath, String uploadId, List<Part> parts)
            throws S3Exception {
        String currentUploadFolder = s3FileObjectPath.getPathToObjectUploadFolder(uploadId);
        if (!isFolderExists(currentUploadFolder)) {
            throw S3Exception.NO_SUCH_UPLOAD(uploadId);
        }
        Path currentUploadFolderPath = Path.of(currentUploadFolder);
        try (Stream<Path> tree = Files.walk(currentUploadFolderPath, 1)) {
            Map<Integer, File> partsFiles = tree
                    .filter(current -> !current.toString().equals(currentUploadFolder) && Files.isRegularFile(current) &&
                            current.getFileName().toString().matches("[0-9]+") &&
                            Integer.parseInt(current.getFileName().toString()) >= 1 &&
                            Integer.parseInt(current.getFileName().toString()) <= 10000)
                    .collect(Collectors.toMap(p -> Integer.parseInt(p.getFileName().toString()), Path::toFile));

            if (!parts.stream().allMatch(partInfo -> partsFiles.containsKey(partInfo.getPartNumber()))) {
                throw S3Exception.INVALID_PART_EXCEPTION();
            }

            File file = new File(s3FileObjectPath.getPathToObject());
            if (!file.exists() || (file.exists() && file.isDirectory())) {
                if (!file.createNewFile()) {
                    throw new IOException("Can't create file");
                }
            }
            MessageDigest md = MessageDigest.getInstance("MD5");
            try (OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file, true))) {
                for (Part part : parts) {
                    File partFile = partsFiles.get(part.getPartNumber());
                    long partSizeInBytes = Files.size(partFile.toPath());
                    if (partSizeInBytes < MIN_PART_SIZE) {
                        if (file.exists()) {
                            file.delete();
                        }
                        throw S3Exception.builder(ENTITY_TOO_SMALL)
                                .setStatus(HttpResponseStatus.BAD_REQUEST)
                                .setCode(S3ResponseErrorCodes.ENTITY_TOO_SMALL)
                                .setMessage(ENTITY_TOO_SMALL)
                                .build();
                    }
                    try (InputStream input = new BufferedInputStream(new FileInputStream(partFile))) {
                        byte[] buffer = input.readAllBytes();
                        String calculatedEtag = DigestUtils.md5Hex(buffer);
                        if (!part.getETag().replaceAll("\"", "").equals(calculatedEtag)) {
                            throw S3Exception.INVALID_PART_EXCEPTION();
                        }
                        outputStream.write(buffer);
                        md.update(buffer);
                    }
                }
            }
            deleteFolder(currentUploadFolder);
            return DatatypeConverter.printHexBinary(md.digest()).toLowerCase();
        } catch (IOException | NoSuchAlgorithmException exception) {
            throw S3Exception.INTERNAL_ERROR(exception);
        }
    }

    private boolean checkPrefix(String prefix, String path) {
        if (prefix == null) {
            return true;
        }
        return path.startsWith(prefix);
    }

    private String calculateETag(byte[] bytes) {
        return DigestUtils.md5Hex(bytes);
    }

    private void checkSelectors(HttpHeaders headers, String eTag, File file) throws ParseException {
        if (headers.contains(S3Headers.IF_MATCH)) {
            ifMatch.check(eTag, headers.get(S3Headers.IF_MATCH).replaceAll("\"", ""));
        }
        if (headers.contains(S3Headers.IF_MODIFIED_SINCE)) {
            ifModifiedSince.check(new Date(file.lastModified()),
                    DateTimeUtil.parseStrTime(headers.get(S3Headers.IF_MODIFIED_SINCE)));
        }
        if (headers.contains(S3Headers.IF_NONE_MATCH)) {
            ifNoneMatch.check(eTag, headers.get(S3Headers.IF_NONE_MATCH).replaceAll("\"", ""));
        }
        if (headers.contains(S3Headers.IF_UNMODIFIED_SINCE)) {
            ifUnmodifiedSince.check(new Date(file.lastModified()),
                    DateTimeUtil.parseStrTime(headers.get(S3Headers.IF_UNMODIFIED_SINCE)));
        }
    }

    private void checkSelectorsCopy(HttpHeaders headers, String eTag, File file) throws ParseException {
        if (headers.contains(S3Headers.IF_MATCH_SOURCE)) {
            ifMatch.check(eTag, headers.get(S3Headers.IF_MATCH_SOURCE).replaceAll("\"", ""));
        }
        if (headers.contains(S3Headers.IF_MODIFIED_SINCE_SOURCE)) {
            ifModifiedSince.check(new Date(file.lastModified()),
                    DateTimeUtil.parseStrTime(headers.get(S3Headers.IF_MODIFIED_SINCE_SOURCE)));
        }
        if (headers.contains(S3Headers.IF_NONE_MATCH_SOURCE)) {
            ifNoneMatch.check(eTag, headers.get(S3Headers.IF_NONE_MATCH_SOURCE).replaceAll("\"", ""));
        }
        if (headers.contains(S3Headers.IF_UNMODIFIED_SINCE_SOURCE)) {
            ifUnmodifiedSince.check(new Date(file.lastModified()),
                    DateTimeUtil.parseStrTime(headers.get(S3Headers.IF_UNMODIFIED_SINCE_SOURCE)));
        }
    }

}
