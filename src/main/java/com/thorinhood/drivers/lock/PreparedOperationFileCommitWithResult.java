package com.thorinhood.drivers.lock;

import java.nio.file.Path;

public class PreparedOperationFileCommitWithResult<T> extends PreparedOperationFileCommit {

    protected T result;

    public PreparedOperationFileCommitWithResult(Path source, Path target, T result, EntityLocker entityLocker) {
        super(source, target, entityLocker);
        this.result = result;
    }

    public T getResult() {
        return result;
    }

}
