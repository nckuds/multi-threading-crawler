package net.spright.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UrlDownloader {
    public static void main(String[] args) throws IOException {
        UrlDownloader urlDown = new UrlDownloader();
        for (String arg : args) {
            UrlSource source = urlDown.download(arg);
            System.out.println(source.getTitle());
        }

    }
    public UrlSource download(
            String urlPath
    ) throws MalformedURLException, IOException {
        URL url = new URL(urlPath);
        URLConnection urlConnection = url.openConnection();
        StringBuilder content = new StringBuilder();
        try (BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(urlConnection.getInputStream()))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                content.append(line)
                        .append("\n");
            }
            return new UrlSource(
                    parseForTitle(content, urlPath), 
                    urlPath, 
                    content.toString()
            );
        }
    }
    private static String parseForTitle(StringBuilder content, String urlPath) {
        int leftIndex = content.indexOf("<title>");
        if (leftIndex == -1) {
            return urlPath;
        }
        int rightIndex = content.indexOf("</title>");
        if (rightIndex == -1) {
            return urlPath;
        }
        return content.substring(leftIndex, rightIndex);
    }
}
