package net.spright.hdfs;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ThreadDemo {
    public static void main(String[] args) throws InterruptedException {
        final int downloadThreadCount = 10;
        final int htmlThreadCount = 10;
        final int flushThreadCount = 10;
        final int urlCount = 1000;
        final int htmlCount = 500;
        final int resultCount = 500;
        ExecutorService service = Executors.newFixedThreadPool(
            downloadThreadCount +
            htmlThreadCount +
            flushThreadCount
        );
        BlockingQueue<String> urlQueue = new ArrayBlockingQueue(urlCount);
        BlockingQueue<Document> htmlQueue = new ArrayBlockingQueue(htmlCount);
        BlockingQueue<HtmlResult> resultQueue = new ArrayBlockingQueue(resultCount);
        urlQueue.put(args[0]);
        /*
        try {
            System.out.println(Jsoup.connect(args[0]).get().outerHtml());
        } catch(IOException ex){
        }
        */
        for (int i = 0; i != downloadThreadCount; ++i) {
            service.execute(new HtmlDownloader(urlQueue, htmlQueue));
        }
        for (int i = 0; i != htmlThreadCount; ++i) {
            service.execute(new HtmlParser(urlQueue, htmlQueue, resultQueue));
        }
        for (int i = 0; i != flushThreadCount; ++i) {
            service.execute(new HtmlResultFlusher(resultQueue));
        }
        service.shutdown();
        waitSomething();
        service.shutdownNow();
        service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        System.exit(0);
    }
    private static void waitSomething() throws InterruptedException {
        TimeUnit.SECONDS.sleep(10);
    }
    private static class HtmlDownloader implements Runnable {
        private final BlockingQueue<String> urlQueue;
        private final BlockingQueue<Document> htmlQueue;
        public HtmlDownloader(
                BlockingQueue<String> urlQueue, 
                BlockingQueue<Document> htmlQueue
        ) {
            this.urlQueue = urlQueue;
            this.htmlQueue = htmlQueue;
        }
        @Override
        public void run() {
            try {
                while (!Thread.interrupted()) {
                    String url = urlQueue.take();
                    //TODO
 //                   System.out.println(url);                   
                    htmlQueue.put(Jsoup.connect(url).get());
                }
            } catch (InterruptedException ex) {
            } catch (IOException ex) {
            }
        }
    }
    private static class HtmlParser implements Runnable {
        private final BlockingQueue<String> urlQueue;
        private final BlockingQueue<Document> htmlQueue;
        private final BlockingQueue<HtmlResult> resultQueue;
        public HtmlParser(BlockingQueue<String> urlQueue, BlockingQueue<Document> htmlQueue, BlockingQueue<HtmlResult> resultQueue) {
            this.urlQueue = urlQueue;
            this.htmlQueue = htmlQueue;
            this.resultQueue = resultQueue;
        }
        @Override
        public void run() {
            try {
                while (!Thread.interrupted()) {
                    Document doc = htmlQueue.take();
                    HtmlResult htmlResult = null;
                    //TODO
                    System.out.println(doc.title() + " : " + doc.location());
                    htmlResult = new HtmlResult(doc.location(), doc.title(), doc.outerHtml());
                    Elements links = doc.select("a[href]");
                    for(Element link : links) {
                        urlQueue.put(link.attr("abs:href"));
                        
                    }
                    resultQueue.put(htmlResult);
                }
            } catch (InterruptedException ex) {
            }
        }
    }
    private static class HtmlResultFlusher implements Runnable {
        private final BlockingQueue<HtmlResult> resultQueue;
        public HtmlResultFlusher(BlockingQueue<HtmlResult> resultQueue) {
            this.resultQueue = resultQueue;
        }
        @Override
        public void run() {
            try {
                while (!Thread.interrupted()) {
                    HtmlResult htmlResult = resultQueue.take();
                    //TODO
                    System.out.println("title: " + htmlResult.title);
                    System.out.println("link: " + htmlResult.link + "\n");
                
                }
            } catch (InterruptedException ex) {
            }
        }
    }
    private static class HtmlResult {
        private final String link;
        private final String title;
        private final String content;
        public HtmlResult(
            String link,
            String title,
            String content
        ) {
            this.link = link;
            this.title = title;
            this.content = content;
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
    }
}
