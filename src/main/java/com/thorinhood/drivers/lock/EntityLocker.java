package com.thorinhood.drivers.lock;

import com.thorinhood.data.S3FileBucketPath;
import com.thorinhood.data.S3FileObjectPath;
import com.thorinhood.drivers.S3Runnable;
import com.thorinhood.drivers.S3Supplier;
import com.thorinhood.exceptions.S3Exception;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class EntityLocker {

    private final Map<String, ReadWriteLock> lockMap;
    private final Set<String> fileLocks;

    public EntityLocker() {
        lockMap = new ConcurrentHashMap<>();
        fileLocks = Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    public void writeBucket(S3FileBucketPath s3FileBucketPath, S3Runnable s3Runnable) throws S3Exception {
        write(s3FileBucketPath.getPathToBucket(), s3Runnable);
    }

    public <T> T completeUpload(S3FileObjectPath s3FileObjectPath, String uploadId, S3Supplier<T> s3Supplier)
            throws S3Exception {
        return read(s3FileObjectPath.getPathToBucket(), () ->
                read(s3FileObjectPath.getPathToBucketMetadataFolder(), () ->
                write(uploadId, () ->
                writeObject(s3FileObjectPath, s3Supplier))));
    }

    public void createUpload(String bucket, String metaFolder, String upload, S3Runnable s3Runnable)
            throws S3Exception {
        read(bucket, () ->
            read(metaFolder, () ->
            write(upload, s3Runnable)));
    }

    public void deleteUpload(String bucket, String metaFolder, String upload, S3Runnable s3Runnable)
            throws S3Exception {
        read(bucket, () ->
            write(metaFolder, () ->
            write(upload, s3Runnable)));
    }

    public <T> T writeUpload(String bucket, String metaFolder, String upload, String part, S3Supplier<T> s3Runnable)
            throws S3Exception {
        return read(bucket, () ->
                read(metaFolder, () ->
                read(upload, () ->
                write(part, s3Runnable))));
    }

    public <T> T writeMeta(String bucket, String metaFolder, String file, S3Supplier<T> s3Supplier) throws S3Exception {
        return read(bucket, () ->
                read(metaFolder, () ->
                write(file, s3Supplier)));
    }

    public void writeMeta(String bucket, String metaFolder, String file, S3Runnable s3Runnable) throws S3Exception {
        read(bucket, () ->
            read(metaFolder, () ->
            write(file, s3Runnable)));
    }

    public <T> T readMeta(String bucket, String metaFolder, String file, S3Supplier<T> s3Supplier) throws S3Exception {
        return read(bucket, () ->
                 read(metaFolder, () ->
                 read(file, s3Supplier)));
    }

    public <T> T readObject(S3FileObjectPath s3FileObjectPath, S3Supplier<T> s3Supplier) throws S3Exception {
        return read(s3FileObjectPath.getPathToBucket(), () ->
                read(s3FileObjectPath.getPathToObject(), () ->
                read(s3FileObjectPath.getPathToObjectMetadataFolder(), () ->
                read(s3FileObjectPath.getPathToObjectMetaFile(), s3Supplier))));
    }

    public void deleteObject(S3FileObjectPath s3FileObjectPath, S3Runnable s3Runnable) throws S3Exception {
        read(s3FileObjectPath.getPathToBucket(), () ->
            write(s3FileObjectPath.getPathToObject(), () ->
            write(s3FileObjectPath.getPathToObjectMetadataFolder(), () ->
            write(s3FileObjectPath.getPathToObjectMetaFile(), () ->
            write(s3FileObjectPath.getPathToObjectAclFile(), s3Runnable)))));
    }

    public <T> T writeObject(S3FileObjectPath s3FileObjectPath, S3Supplier<T> s3Supplier) throws S3Exception {
        return read(s3FileObjectPath.getPathToBucket(), () ->
                write(s3FileObjectPath.getPathToObject(), () ->
                read(s3FileObjectPath.getPathToObjectMetadataFolder(), () ->
                write(s3FileObjectPath.getPathToObjectMetaFile(), () ->
                write(s3FileObjectPath.getPathToObjectAclFile(), s3Supplier)))));
    }

    private <T> T write(String key, S3Supplier<T> s3Supplier) throws S3Exception {
        ReadWriteLock lock = getLock(key);
        try {
            lock.writeLock().lock();
            return supplier(key, s3Supplier);
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void write(String key, S3Runnable s3Runnable) throws S3Exception {
        ReadWriteLock lock = getLock(key);
        try {
            lock.writeLock().lock();
            runnable(key, s3Runnable);
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private <T> T read(String key, S3Supplier<T> s3Supplier) throws S3Exception {
        ReadWriteLock lock = getLock(key);
        try {
            lock.readLock().lock();
            return supplier(key, s3Supplier);
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void read(String key, S3Runnable s3Runnable) throws S3Exception {
        ReadWriteLock lock = getLock(key);
        try {
            lock.readLock().lock();
            runnable(key, s3Runnable);
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void runnable(String key, S3Runnable s3Runnable) throws IOException {
        File file = new File(key);
        if (file.isFile()) {
            if (!fileLocks.contains(key)) {
                try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ,
                     StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                     FileLock fileLock = fileChannel.lock(0, Long.MAX_VALUE, true)) {
                    fileLocks.add(key);
                    s3Runnable.run();
                } catch(OverlappingFileLockException exception) {
                    s3Runnable.run();
                } finally {
                    fileLocks.remove(key);
                }
            } else {
                s3Runnable.run();
            }
        } else {
            s3Runnable.run();
        }
    }

    private <T> T supplier(String key, S3Supplier<T> s3Supplier) throws IOException {
        File file = new File(key);
        if (file.isFile()) {
            if (!fileLocks.contains(key)) {
                try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                     FileLock fileLock = fileChannel.lock(0, Long.MAX_VALUE, true)) {
                    fileLocks.add(key);
                    return s3Supplier.get();
                } catch (OverlappingFileLockException exception) {
                    return s3Supplier.get();
                } finally {
                    fileLocks.remove(key);
                }
            } else {
                return s3Supplier.get();
            }
        } else {
            return s3Supplier.get();
        }
    }

    private ReadWriteLock getLock(String key) {
        ReadWriteLock lock = lockMap.get(key);
        if (lock != null) {
            return lock;
        }
        lock = new ReentrantReadWriteLock();
        ReadWriteLock current = lockMap.putIfAbsent(key, lock);
        return current == null ? lock : current;
    }
}
