package byteplus.rec.core;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class URLCenter {
    private static final Map<String, URLCenter> hostURLCenterMap = new HashMap<>();
    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private static final ReentrantReadWriteLock.ReadLock hostReadLock = lock.readLock();
    private static final ReentrantReadWriteLock.WriteLock hostWriteLock = lock.writeLock();

    private String urlFormat;
    private Map<String, String> pathURLMap;
    private ReentrantReadWriteLock.ReadLock pathReadLock;
    private ReentrantReadWriteLock.WriteLock pathWriteLock;

    protected static URLCenter newInstance(String schema, String host) {
        String key = String.format("%s_%s", schema, host);
        hostReadLock.lock();
        URLCenter urlCenter = hostURLCenterMap.get(key);
        hostReadLock.unlock();
        if (urlCenter != null) {
            return urlCenter;
        }
        hostWriteLock.lock();
        urlCenter = hostURLCenterMap.get(key);
        if (urlCenter == null) {
            urlCenter = new URLCenter(schema, host);
            hostURLCenterMap.put(key, urlCenter);
        }
        hostWriteLock.unlock();
        return urlCenter;
    }

    private URLCenter(String schema, String host) {
        urlFormat = String.format("%s://%s", schema, host);
        pathURLMap = new HashMap<>();
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        pathReadLock = lock.readLock();
        pathWriteLock = lock.writeLock();
    }

    // example path: /Retail/User
    // will build url to schema://host/Retail/User
    protected String getURL(String path) {
        while (path.startsWith("/")) {
            path = path.substring(1);
        }
        pathReadLock.lock();
        String url = pathURLMap.get(path);
        pathReadLock.unlock();
        if (url != null) {
            return url;
        }
        pathWriteLock.lock();
        url = pathURLMap.get(path);
        if (url == null) {
            url = String.format("%s/%s", urlFormat, path);
            pathURLMap.put(path, url);
        }
        pathWriteLock.unlock();
        return url;
    }
}
