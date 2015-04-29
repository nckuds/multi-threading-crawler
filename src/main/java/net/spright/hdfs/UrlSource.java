package net.spright.hdfs;

import java.io.Closeable;
import java.io.IOException;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class UrlSource {
    public static interface Writer extends Closeable {
        public void write(UrlSource source) throws IOException;
    }
    public static interface Scanner extends Iterable<UrlSource>, Closeable {
    } 
    private final String title;
    private final String link;
    private final String content;
    private final int hashCode;
    public UrlSource(String title, String link, String content) {
        this.title = title;
        this.link = link;
        this.content = content;
        hashCode = new HashCodeBuilder()
                .append(title)
                .append(link)
                .toHashCode();
    }
    public String getTitle() {
        return title;
    }
    public String getLink() {
        return link;
    }
    public String getContent() {
        return content;
    }
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        return ((UrlSource)obj).getLink().equals(getLink());
    }
    @Override
    public int hashCode() {
        return hashCode;
    }
    @Override
    public String toString() {
        return content;
    }
}
