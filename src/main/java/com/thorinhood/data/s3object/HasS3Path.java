package com.thorinhood.data.s3object;

import com.thorinhood.data.S3FileObjectPath;

public interface HasS3Path {
    HasETag setS3Path(S3FileObjectPath s3FileObjectPath);
    S3FileObjectPath getS3Path();
}
