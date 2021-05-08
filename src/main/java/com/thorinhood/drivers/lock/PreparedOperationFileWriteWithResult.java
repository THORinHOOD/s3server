package com.thorinhood.drivers.lock;

import java.nio.file.Path;

public class PreparedOperationFileWriteWithResult<T> extends PreparedOperationFileWrite {

    protected T result;

    public PreparedOperationFileWriteWithResult(Path source, Path target, T result, EntityLocker entityLocker) {
        super(source, target, entityLocker);
        this.result = result;
    }

    public T getResult() {
        return result;
    }

}
