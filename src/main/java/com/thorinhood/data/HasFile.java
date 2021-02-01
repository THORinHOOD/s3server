package com.thorinhood.data;

import java.io.File;

public interface HasFile {
    HasRawBytes setFile(File file);
    File getFile();
}
