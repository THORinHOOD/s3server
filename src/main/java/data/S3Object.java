package data;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.*;
import java.nio.file.Files;
import java.util.Optional;

public class S3Object {

    private String absolutePath;
    private String key;
    private String ETag;
    private File file;
    private byte[] bytes;

    public static S3Object get(String bucket, String key, String basePath) throws IOException {
        Optional<String> absolutePath = buildPath(bucket, key, basePath);
        if (absolutePath.isEmpty()) {
            //TODO
            return null;
        }
        File file = new File(absolutePath.get());
        if (file.isHidden() || !file.exists()) {
            //TODO
            return null;
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
                .setBytes(bytes);
    }

    public static S3Object save(String bucket, String key, String basePath, byte[] bytes) throws IOException {
        Optional<String> absolutePath = buildPath(bucket, key, basePath);
        if (absolutePath.isEmpty()) {
            //TODO
            return null;
        }

        File file = new File(absolutePath.get());
        if (file.exists()) {
            //TODO
            return null;
        }
        if (file.createNewFile()) {
            FileOutputStream outputStream = new FileOutputStream(file);
            outputStream.write(bytes);
            outputStream.close();
            return new S3Object()
                    .setAbsolutePath(absolutePath.get())
                    .setKey(key)
                    .setETag(calculateETag(bytes))
                    .setFile(file)
                    .setBytes(bytes);
        } else {
            //TODO
            return null;
        }
    }

    private S3Object() {
    }

    //TODO
    private static Optional<String> buildPath(String bucket, String key, String basePath) {
        if (key == null || bucket == null) {
            return Optional.empty();
        }
        return Optional.of(basePath + File.separatorChar + bucket + key);
    }

    private static String calculateETag(byte[] bytes) {
        return DigestUtils.md5Hex(bytes);
    }

    public String getAbsolutePath() {
        return absolutePath;
    }

    public String getKey() {
        return key;
    }

    public String getETag() {
        return ETag;
    }

    public File getFile() {
        return file;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public S3Object setAbsolutePath(String absolutePath) {
        this.absolutePath = absolutePath;
        return this;
    }

    public S3Object setKey(String key) {
        this.key = key;
        return this;
    }

    public S3Object setETag(String ETag) {
        this.ETag = ETag;
        return this;
    }

    public S3Object setFile(File file) {
        this.file = file;
        return this;
    }

    public S3Object setBytes(byte[] bytes) {
        this.bytes = bytes;
        return this;
    }
}
