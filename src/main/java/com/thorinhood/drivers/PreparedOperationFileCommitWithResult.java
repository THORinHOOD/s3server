package com.thorinhood.drivers;

import java.nio.file.Path;

public class PreparedOperationFileCommitWithResult<T> extends PreparedOperationFileCommit {

    protected T result;

    public PreparedOperationFileCommitWithResult(Path source, Path target, T result) {
        super(source, target);
        this.result = result;
    }

    public T getResult() {
        return result;
    }

}
