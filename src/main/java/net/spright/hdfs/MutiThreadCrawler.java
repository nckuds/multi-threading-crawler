package net.spright.hdfs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jsoup.HttpStatusException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class MutiThreadCrawler {



    public static void main(String[] args) throws InterruptedException {
        final int downloadThreadCount = 10;
        final int htmlThreadCount = 10;
        final int flushThreadCount = 10;
        final int urlCount = 500;
        final int htmlCount = 250;
        final int resultCount = 250;


        ExecutorService service = Executors.newFixedThreadPool(
            downloadThreadCount +
            htmlThreadCount +
            flushThreadCount
        );
        BlockingQueue<String> urlQueue = new ArrayBlockingQueue(urlCount);
        BlockingQueue<Document> htmlQueue = new ArrayBlockingQueue(htmlCount);
        BlockingQueue<HtmlResult> resultQueue = new ArrayBlockingQueue(resultCount);
        urlQueue.put(args[0]);

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
        private final static ArrayList<String> urlList =  new ArrayList<String>();

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
                    //TODO
                    while (urlQueue.isEmpty()) {
                        if (urlQueue.isEmpty() && htmlQueue.isEmpty()){
                            break;
                        }
                    } // waiting

                    String url = urlQueue.take();
                    if (!urlList.contains(url)) {
                       
                        try {
                            Document doc = Jsoup.connect(url).get();  
                            htmlQueue.put(doc);
                        }
                        catch (IllegalArgumentException | HttpStatusException e) {
                            //System.out.println("url is not vaild");
                            //System.out.println(e);
                        }
                      
                        urlList.add(url);
                    }
                }
            } catch (InterruptedException | IOException e) {
                //System.out.println("urlQueue interrupted");
                Thread.currentThread().interrupt(); 
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
                    while (htmlQueue.isEmpty()) {
                        if (urlQueue.isEmpty() && htmlQueue.isEmpty()) {
                            break;
                        }
                    } // waiting

                    Document doc = htmlQueue.take();
                    HtmlResult htmlResult = null;
                 
                    //System.out.println(doc.title() + " : " + doc.location());
                    htmlResult = new HtmlResult(doc.location(), doc.title(), doc.outerHtml());
                    Elements links = doc.select("a[href]");
                    resultQueue.put(htmlResult);
                    for(Element link : links) {
                        urlQueue.put(link.attr("abs:href"));
                    }
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt(); 
            }
        }
    }
    private static class HtmlResultFlusher implements Runnable {
 
        private final BlockingQueue<HtmlResult> resultQueue;
        
        private static int pageID = 0;
        public HtmlResultFlusher(BlockingQueue<HtmlResult> resultQueue) {
            this.resultQueue = resultQueue;
        }
        @Override
        public void run() {

            try {
                while (!Thread.interrupted()) {

                    //while (resultQueue.isEmpty()) {} // waiting
                  
                    HtmlResult htmlResult = resultQueue.take();
                    System.out.println("title: " + htmlResult.title);
                    System.out.println("link: " + htmlResult.link + "\n");
                    //outputHtml(htmlResult,  "page_" + htmlResult.title);
                    pageID+=1;
                    
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt(); 
            }


        }

        /*public static void outputHtml(HtmlResult htmlResult, String outputPath) {
            Configuration configuration = new Configuration();
               try (FileSystem fs = FileSystem.get(configuration)) {
                   try (BufferedWriter writer = new BufferedWriter(
                      new OutputStreamWriter(
                       fs.create(new Path(outputPath))
                           , Charset.forName("UTF-8")))) {
                               writer.write(htmlResult.link + "\n");
                               writer.write(htmlResult.title + "\n");
                               writer.write(htmlResult.content);
                           }
                   } catch (IOException ex) {
               Logger.getLogger(MutiThreadCrawler.class.getName()).log(Level.SEVERE, null, ex);
           }
        }*/
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
