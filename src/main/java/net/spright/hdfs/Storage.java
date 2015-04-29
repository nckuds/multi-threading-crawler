package net.spright.hdfs;

import java.io.Closeable;
import java.io.IOException;

public interface Storage extends Closeable {
    public abstract UrlSource.Writer createWriter() throws IOException;
    public abstract UrlSource.Scanner createScanner() throws IOException;
}
