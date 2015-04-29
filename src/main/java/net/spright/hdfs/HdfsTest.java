package net.spright.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import static java.lang.System.exit;
import java.nio.charset.Charset;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsTest {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsTest.class);
    private static final int BUFFER_SIZE = 1024;
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("Need {source} and {dest}");
            exit(0);
        }
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Configuration configuration = new Configuration();
        try (FileSystem fs = FileSystem.get(configuration)) {
            inputPath = initInput(fs, inputPath);
            outputPath = initOutput(fs, outputPath);
            copyAll(fs, inputPath, outputPath);
        }
    }
    private static Path getRelativePath(Path base, Path path) {
        String baseString = base.toString();
        String pathString = path.toString();
        if (baseString.length() >= pathString.length()) {
            throw new IllegalArgumentException(
                    "The base must be bigger than path"
            );
        }
        int index = pathString.indexOf(baseString);
        if (index == -1) {
            throw new IllegalArgumentException("Error base");
        }
        String rvalString = pathString.substring(
                baseString.length() + index,
                pathString.length()
        );
        if (rvalString.charAt(0) == '/') {
            return new Path(rvalString.substring(1, rvalString.length())); 
        } else {
            return new Path(rvalString);
        }
        
    }
    private static void testWrite() throws IOException {
        Configuration configuration = new Configuration();
        try (FileSystem fs = FileSystem.get(configuration)) {
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(
                            fs.create(new Path("your path"))
                            , Charset.forName("UTF-8")))) {
                String line = null;
                //產生你的字串
		writer.write(line);
            }
        }
    }
    private static void testRead() throws IOException {
        Configuration configuration = new Configuration();
        try (FileSystem fs = FileSystem.get(configuration)) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(
                            fs.open(new Path("your path"))
                            , Charset.forName("UTF-8")))) {
                String line = null;
		while((line = reader.readLine()) != null) {
			//處理你的字串
		}
            }
        }
    }
    private static void testList() throws IOException {
        Configuration configuration = new Configuration();
        try (FileSystem fs = FileSystem.get(configuration)) {
            RemoteIterator<LocatedFileStatus> iter = fs.listFiles(
                    new Path("your path"), 
                    true
            );
            while (iter.hasNext()) {
                LocatedFileStatus status = iter.next();
                Path path = status.getPath();
                //從此處開始處理檔案
            }
        }
    }
    private static void copyAll(
            FileSystem fs,
            Path inputPath,
            Path outputPath
    ) throws IOException {
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(inputPath, true);
        while (iter.hasNext()) {
            LocatedFileStatus status = iter.next();
            if (status.isDirectory()) {
                fs.mkdirs(new Path(
                        outputPath, 
                        getRelativePath(inputPath, status.getPath()))
                );
            }
            if (status.isFile()) {
                FSDataOutputStream output = null;
                FSDataInputStream input = null;
                try {
                    input = fs.open(status.getPath());
                    Path destPath = new Path(
                            outputPath, 
                            getRelativePath(inputPath, status.getPath())
                    );
                    output = fs.create(destPath);
                    LOG.info("copy file from " + status.getPath() 
                            + " to " + destPath
                    );
                    byte[] buf = new byte[BUFFER_SIZE];
                    int rval;
                    while ((rval = input.read(buf)) != -1) {
                        output.write(buf, 0, rval);
                    }
                } finally {
                    IOException ex = null;
                    if (output != null) {
                        try {
                            output.close();
                        } catch(IOException e) {
                            ex = e;
                        }
                    }
                    if (input != null) {
                        try {
                            input.close();
                        } catch(IOException e) {
                            ex = e;
                        }
                    }
                    if (ex != null) {
                        throw ex;
                    }
                }
            }
        }
    }
    private static Path initInput(FileSystem fs, Path inputPath
    ) throws IOException {
        inputPath = inputPath.makeQualified(
                fs.getUri(), 
                fs.getWorkingDirectory());
        LOG.info("input path = " + inputPath);
        if (fs.isDirectory(inputPath)) {
            return inputPath;
        }
        throw new IOException(
                "The source must be directory : " + inputPath.toString()
        );
    }
    private static Path initOutput(
            FileSystem fs, 
            Path outputPath
    ) throws IOException {
        outputPath = outputPath.makeQualified(
                fs.getUri(), 
                fs.getWorkingDirectory()
        );
        LOG.info("output path = " + outputPath);
        if (fs.exists(outputPath)) {
            throw new IOException("The dest is exised : " + outputPath);
        } else {
            fs.mkdirs(outputPath);
            return outputPath;
        }
    }
}
