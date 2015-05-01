# multi-threading-crawler

A multi-threading crawler parses html title, link, content and store them to hdfs

## Usage

Fisrt, compile `MutiThreadCrawler.java` and get the output `jar` file, then type

```
$ hadoop jar Crawler-0.1.jar net.spright.hdfs.MutiThreadCrawler <URL> <seconds>  /* seconds for shutdown */
```
