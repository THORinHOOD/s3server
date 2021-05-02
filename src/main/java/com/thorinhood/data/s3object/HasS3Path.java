package com.thorinhood.data.s3object;

import com.thorinhood.data.S3ObjectPath;

public interface HasS3Path {
    HasETag setS3Path(S3ObjectPath s3ObjectPath);
    S3ObjectPath getS3Path();
}
