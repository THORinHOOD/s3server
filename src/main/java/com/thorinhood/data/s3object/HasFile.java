package com.thorinhood.data.s3object;

import java.io.File;

public interface HasFile {
    HasRawBytes setFile(File file);
    File getFile();
}
