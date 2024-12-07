package org.dflib.iceberg;

import org.dflib.DataFrame;

import java.io.File;
import java.nio.file.Path;

public class Iceberg {

    public static DataFrame load(File file) {
        return loader().load(file);
    }

    public static DataFrame load(Path filePath) {
        return loader().load(filePath);
    }

    public static DataFrame load(String filePath) {
        return loader().load(filePath);
    }

    public static IcebergLoader loader() {
        return new IcebergLoader();
    }

}
