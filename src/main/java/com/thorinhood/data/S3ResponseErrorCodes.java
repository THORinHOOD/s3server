package com.thorinhood.data;

public class S3ResponseErrorCodes {
    public static final String INTERNAL_ERROR = "InternalError";
    public static final String BUCKET_ALREADY_OWNED_BY_YOU = "BucketAlreadyOwnedByYou";
    public static final String BUCKET_ALREADY_EXISTS = "BucketAlreadyExists";
    public static final String NO_SUCH_KEY = "NoSuchKey";
    public static final String INVALID_REQUEST = "InvalidRequest";
    public static final String INVALID_ARGUMENT = "InvalidArgument";
    public static final String SIGNATURE_DOES_NOT_MATCH = "SignatureDoesNotMatch";
    public static final String PRECONDITION_FAILED = "PreconditionFailed";
    public static final String ACCESS_DENIED = "AccessDenied";
}
