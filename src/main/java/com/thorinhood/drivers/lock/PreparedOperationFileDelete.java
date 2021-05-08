package com.thorinhood.drivers.lock;

import com.thorinhood.exceptions.S3Exception;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class PreparedOperationFileDelete extends PreparedOperationFileAbstract {

    public PreparedOperationFileDelete(Path target, EntityLocker entityLocker) {
        super(target, entityLocker);
    }

    @Override
    protected void commit() throws S3Exception {
        try {
            if (target.toFile().exists()) {
                Files.delete(target);
            }
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception)
                    .setResource("1")
                    .setRequestId("1"); // TODO
        }
    }
}
