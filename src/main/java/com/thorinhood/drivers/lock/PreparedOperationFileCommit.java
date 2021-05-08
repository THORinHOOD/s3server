package com.thorinhood.drivers.lock;

import com.thorinhood.drivers.S3Runnable;
import com.thorinhood.exceptions.S3Exception;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

public class PreparedOperationFileCommit {

    protected Path target;
    protected Path source;
    protected EntityLocker entityLocker;

    public PreparedOperationFileCommit(Path source, Path target, EntityLocker entityLocker) {
        this.target = target;
        this.source = source;
        this.entityLocker = entityLocker;
    }

    public void lockAndCommitAfter(S3Runnable s3runnable) throws S3Exception {
        entityLocker.write(target.toAbsolutePath().toString(), () -> {
            try (FileLock lock = FileChannel.open(target, StandardOpenOption.WRITE, StandardOpenOption.READ,
                    StandardOpenOption.CREATE).lock()) {
                s3runnable.run();
                commit();
            } catch (S3Exception e) {
                throw e;
            } catch (Exception exception) {
                throw S3Exception.INTERNAL_ERROR(exception)
                        .setResource("1")
                        .setRequestId("1");
            }
        });
    }

    public void lockAndCommit() throws S3Exception {
        entityLocker.write(target.toAbsolutePath().toString(), () -> {
            try (FileLock fileLock = FileChannel.open(target, StandardOpenOption.WRITE, StandardOpenOption.READ,
                    StandardOpenOption.CREATE).lock()) {
                commit();
            } catch (Exception exception) {
                throw S3Exception.INTERNAL_ERROR(exception)
                        .setResource("1")
                        .setRequestId("1");
            }
        });
    }

    private void commit() throws S3Exception {
        try {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception)
                    .setResource("1")
                    .setRequestId("1"); // TODO
        }
    }

}
