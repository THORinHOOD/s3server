package com.thorinhood.exceptions;

import com.thorinhood.data.S3FileBucketPath;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import io.netty.handler.codec.http.HttpResponseStatus;

public class S3Exception extends RuntimeException {

    private static final String INVALID_PART_MESSAGE = "One or more of the specified parts could not be found. " +
            "The part might not have been uploaded, or the specified entity tag might not " +
            "have matched the part's entity tag.";

    protected HttpResponseStatus status;
    protected String code;
    protected String message;

    public static S3Exception ACCESS_DENIED() {
        return builder("Access denied")
                .setStatus(HttpResponseStatus.FORBIDDEN)
                .setCode(S3ResponseErrorCodes.ACCESS_DENIED)
                .setMessage("Access denied")
                .build();
    }

    public static S3Exception INTERNAL_ERROR(Exception exception) {
        return INTERNAL_ERROR(exception.getMessage() != null ? exception.getMessage() : exception.toString());
    }

    public static S3Exception INTERNAL_ERROR(String message) {
        return builder(message)
                .setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                .setCode(S3ResponseErrorCodes.INTERNAL_ERROR)
                .setMessage(message)
                .build();
    }

    public static S3Exception BUCKET_ALREADY_OWNED_BY_YOU(String bucketPath) {
        return S3Exception.builder("Bucket already exists : " + bucketPath)
                .setStatus(HttpResponseStatus.CONFLICT)
                .setCode(S3ResponseErrorCodes.BUCKET_ALREADY_OWNED_BY_YOU)
                .setMessage("Your previous request to create the named bucket succeeded and " +
                        "you already own it.")
                .build();
    }

    public static S3Exception BUCKET_ALREADY_EXISTS() {
        return S3Exception.builder("The specified location-constraint is not valid")
                .setStatus(HttpResponseStatus.CONFLICT)
                .setCode(S3ResponseErrorCodes.BUCKET_ALREADY_EXISTS)
                .setMessage("The requested bucket name is not available. " +
                        "The bucket namespace is shared by all users of the system. " +
                        "Please select a different name and try again.")
                .build();
    }

    public static S3Exception INVALID_PART_EXCEPTION() {
        return S3Exception.builder(INVALID_PART_MESSAGE)
                .setStatus(HttpResponseStatus.BAD_REQUEST)
                .setCode(S3ResponseErrorCodes.INVALID_PART)
                .setMessage(INVALID_PART_MESSAGE)
                .build();
    }

    public static S3Exception NO_SUCH_UPLOAD(String uploadId) {
        return builder("No such upload : " + uploadId)
                .setStatus(HttpResponseStatus.NOT_FOUND)
                .setCode(S3ResponseErrorCodes.NO_SUCH_UPLOAD)
                .setMessage("The specified multipart upload does not exist. The upload ID might be invalid, " +
                        "or the multipart upload might have been aborted or completed.")
                .build();
    }

    public static Builder builder(String internalMessage) {
        return new Builder(internalMessage);
    }

    protected S3Exception(String message) {
        super(message);
    }

    public HttpResponseStatus getStatus() {
        return status;
    }

    public String getCode() {
        return code;
    }

    public String getS3Message() {
        return message;
    }

    public static class Builder {
        private final S3Exception s3Exception;

        public Builder(String message) {
            s3Exception = new S3Exception(message);
        }

        public Builder setMessage(String message) {
            s3Exception.message = message;
            return this;
        }

        public Builder setStatus(HttpResponseStatus status) {
            s3Exception.status = status;
            return this;
        }

        public Builder setCode(String code) {
            s3Exception.code = code;
            return this;
        }

        public S3Exception build() {
            return s3Exception;
        }

    }

}
