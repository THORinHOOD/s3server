package com.thorinhood.drivers.lock;

import com.thorinhood.exceptions.S3Exception;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class PreparedOperationFileWrite extends PreparedOperationFileAbstract {

    protected Path source;

    public PreparedOperationFileWrite(Path source, Path target, EntityLocker entityLocker) {
        super(target, entityLocker);
        this.source = source;
    }

    protected void commit() throws S3Exception {
        try {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception)
                    .setResource("1")
                    .setRequestId("1"); // TODO
        }
    }

}
