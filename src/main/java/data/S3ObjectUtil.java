package data;

import exceptions.S3Exception;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.codec.digest.DigestUtils;
import utils.DateTimeUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class S3ObjectUtil {

    public static S3Object get(String bucket, String key, String basePath) throws IOException, S3Exception {
        Optional<String> absolutePath = buildPath(bucket, key, basePath);
        if (absolutePath.isEmpty()) {
            //TODO
            return null;
        }
        File file = new File(absolutePath.get());
        if (file.isHidden() || !file.exists()) {
            throw new S3Exception("File not found: " + absolutePath)
                    .setStatus(HttpResponseStatus.NOT_FOUND)
                    .setCode("NoSuchKey")
                    .setMessage("The resource you requested does not exist")
                    .setResource(File.separatorChar + bucket + key)
                    .setRequestId("1");
        }
        if (!file.isFile()) {
            //TODO
            return null;
        }
        byte[] bytes = Files.readAllBytes(file.toPath());
        return new S3Object()
                .setETag(calculateETag(bytes))
                .setKey(key)
                .setAbsolutePath(absolutePath.get())
                .setFile(file)
                .setBytes(bytes)
                .setLastModified(DateTimeUtil.parseDateTime(file));
    }

    public static S3Object save(String bucket, String key, String basePath, byte[] bytes) throws IOException {
        Optional<String> absolutePath = buildPath(bucket, key, basePath);
        if (absolutePath.isEmpty()) {
            //TODO
            return null;
        }
        File file = new File(absolutePath.get());
        if (!processFolders(file, bucket)) {
            throw new S3Exception("Can't create folders: " + absolutePath)
                    .setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                    .setMessage("Internal error : can't create folder")
                    .setCode("InternalError")
                    .setResource(File.separatorChar + bucket + key)
                    .setRequestId("1");
        }
        if (file.createNewFile() || file.exists()) {
            FileOutputStream outputStream = new FileOutputStream(file);
            outputStream.write(bytes);
            outputStream.close();
            return new S3Object()
                    .setAbsolutePath(absolutePath.get())
                    .setKey(key)
                    .setETag(calculateETag(bytes))
                    .setFile(file)
                    .setBytes(bytes)
                    .setLastModified(DateTimeUtil.parseDateTime(file));
        } else {
            throw new S3Exception("Can't create object: " + absolutePath)
                    .setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                    .setMessage("Internal error : can't create object")
                    .setCode("InternalError")
                    .setResource(File.separatorChar + bucket + key)
                    .setRequestId("1");
        }
    }

    //TODO
    private static Optional<String> buildPath(String bucket, String key, String basePath) {
        if (key == null || bucket == null) {
            return Optional.empty();
        }
        return Optional.of(basePath + File.separatorChar + bucket + key);
    }

    private static boolean processFolders(File file, String bucket) {
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

    private static String calculateETag(byte[] bytes) {
        return DigestUtils.md5Hex(bytes);
    }

}
