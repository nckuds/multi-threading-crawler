package net.spright.hdfs;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.LoggerFactory;

public abstract class StorageFactory implements Closeable {
    private static final Charset ENCODE = Charset.forName("UTF-8");
    public static Storage getDefault(
            Configuration configuration
           , String root
    ) throws IOException {
        return new HdfsStorage(configuration, root);
    }
    private static class HdfsStorage implements Storage {
        private static final org.slf4j.Logger LOG = 
                LoggerFactory.getLogger(HdfsStorage.class);
        private final FileSystem fs;
        private final String root;
        public HdfsStorage(Configuration configuration, String root) throws IOException {
            fs = FileSystem.get(configuration);
            this.root = root;
        }
        @Override
        public UrlSource.Writer createWriter() {
            return new WriterImpl(fs, root);
        }
        @Override
        public UrlSource.Scanner createScanner() throws IOException {
            return new ScannerImpl(fs, root);
        }
        @Override
        public void close() throws IOException {
            fs.close();
        }

        private static class ScannerImpl implements UrlSource.Scanner {
            private final FileSystem fs;
            private final List<Path> rawDataPath = new LinkedList();
            public ScannerImpl(FileSystem fs, String root) throws IOException {
                this.fs = fs;
                RemoteIterator<LocatedFileStatus> iter = fs.listFiles(
                    new Path(root), 
                    true
                );
                while (iter.hasNext()) {
                    LocatedFileStatus status = iter.next();
                    rawDataPath.add(status.getPath());
                }
            }

            @Override
            public Iterator<UrlSource> iterator() {
                return new Iterator<UrlSource>() {
                    private int index = 0;
                    private DataInputStream input = null;
                    @Override
                    public boolean hasNext() {
                        while (true) {
                            if (index >= rawDataPath.size()) {
                                return false;
                            }
                            try {
                                input = fs.open(rawDataPath.get(index++));
                                return true;
                            } catch (IOException e) {
                                LOG.error(e.getMessage());
                            }
                        }
                    }
                    @Override
                    public UrlSource next() {
                        if (input == null) {
                            LOG.error("Hdfs scanner error when iteratot");
                            return null;
                        }
                        try (DataInputStream localInput = input) {
                            int titleLength = localInput.readInt();
                            int linkLength = localInput.readInt();
                            int contentLength = localInput.readInt();
                            byte[] buffer = new byte[
                                    titleLength 
                                    + linkLength
                                    + contentLength];
                            int sum = 0;
                            while (sum != buffer.length) {
                                int rval = localInput.read(buffer, sum, buffer.length - sum);
                                if (rval == -1 ) {
                                    LOG.error(
                                            "error format : rval = " + rval 
                                          + ", should be : " + buffer.length);
                                    return null;
                                }
                                sum += rval;
                            }
                            return new UrlSource(
                                    new String(
                                            buffer, 
                                            0, 
                                            titleLength,
                                            ENCODE)
                                    , new String(
                                            buffer, 
                                            titleLength, 
                                            linkLength,
                                            ENCODE)
                                    , new String(
                                            buffer, 
                                            titleLength + linkLength, 
                                            contentLength,
                                            ENCODE)
                            );
                        } catch (IOException ex) {
                            LOG.error(ex.getMessage());
                            return null;
                        } finally {
                            input = null;
                        }
                    }
                };
            }

            @Override
            public void close() throws IOException {
            }
        }
        private static class WriterImpl implements UrlSource.Writer {
            private final FileSystem fs;
            private final String root;
            public WriterImpl(FileSystem fs, String root) {
                this.fs = fs;
                this.root = root;
            }
            @Override
            public void write(UrlSource source) throws IOException {
                byte[] titleBytes = source.getTitle().getBytes(ENCODE);
                byte[] linkBytes = source.getLink().getBytes(ENCODE);
                byte[] contentBytes = source.getContent().getBytes(ENCODE);
                try (DataOutputStream output = fs.create(
                        normalizePath(source))) {
                    output.writeInt(titleBytes.length);
                    output.writeInt(linkBytes.length);
                    output.writeInt(contentBytes.length);
                    output.write(titleBytes);
                    output.write(linkBytes);
                    output.write(contentBytes);
                }
            }
            @Override
            public void close() throws IOException {
            }
            private Path normalizePath(UrlSource source) {
                return new Path(root, String.valueOf(source.hashCode()));
            }
        }
    }
}
