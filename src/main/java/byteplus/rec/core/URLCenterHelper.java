package byteplus.rec.core;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class URLCenterHelper {
    private URLCenterHelper(){}
    private static final Map<String, URLCenter> hostURLCenterMap = new HashMap<>();
    private static final ReentrantReadWriteLock urlCenterLock = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock urlCenterReadLock = urlCenterLock.readLock();
    private static final ReentrantReadWriteLock.WriteLock urlCenterWriteLock = urlCenterLock.writeLock();

    protected static URLCenter urlCenterInstance(String schema, String host) {
        String key = String.format("%s_%s", schema, host);
        urlCenterReadLock.lock();
        URLCenter urlCenter = hostURLCenterMap.get(key);
        urlCenterReadLock.unlock();
        if (urlCenter != null) {
            return urlCenter;
        }
        urlCenterWriteLock.lock();
        urlCenter = hostURLCenterMap.get(key);
        if (urlCenter == null) {
            urlCenter = new URLCenter(schema, host);
            hostURLCenterMap.put(key, urlCenter);
        }
        urlCenterWriteLock.unlock();
        return urlCenter;
    }

    protected static class URLCenter{
        private String urlFormat;
        private Map<String, String> pathURLMap;
        private ReentrantReadWriteLock.ReadLock readLock;
        private ReentrantReadWriteLock.WriteLock writeLock;

        private URLCenter(String schema, String host) {
            urlFormat = String.format("%s://%s", schema, host);
            pathURLMap = new HashMap<>();
            ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
            readLock = lock.readLock();
            writeLock = lock.writeLock();
        }

        // example path: /Retail/User
        // will build url to schema://host/Retail/User
        protected String getURL(String path) {
            while (path.startsWith("/")) {
                path = path.substring(1);
            }
            readLock.lock();
            String url = pathURLMap.get(path);
            readLock.unlock();
            if (url != null) {
                return url;
            }
            writeLock.lock();
            url = pathURLMap.get(path);
            if (url == null) {
                url = String.format("%s/%s", urlFormat, path);
                pathURLMap.put(path, url);
            }
            writeLock.unlock();
            return url;
        }
    }
}
