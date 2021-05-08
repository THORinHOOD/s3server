package com.thorinhood.drivers.lock;

import com.thorinhood.drivers.S3Runnable;
import com.thorinhood.drivers.S3Supplier;
import com.thorinhood.exceptions.S3Exception;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class EntityLocker {

    private final Map<String, ReadWriteLock> lockMap;

    public EntityLocker() {
        lockMap = new ConcurrentHashMap<>();
    }

    public void write(String key, S3Runnable s3Runnable) throws S3Exception {
        ReadWriteLock lock = getLock(key);
        try {
            lock.writeLock().lock();
            s3Runnable.run();
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception)
                    .setResource("1")
                    .setRequestId("1");
        } finally {
            lock.writeLock().unlock();
        }
    }

    public <T> T read(String key, S3Supplier<T> s3Supplier) throws S3Exception {
        ReadWriteLock lock = getLock(key);
        try {
            lock.readLock().lock();
            return s3Supplier.get();
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception)
                    .setResource("1")
                    .setRequestId("1");
        } finally {
            lock.readLock().unlock();
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
