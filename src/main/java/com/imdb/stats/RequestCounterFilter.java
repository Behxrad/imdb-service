package com.imdb.stats;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class RequestCounterFilter implements Filter {

    private final AtomicLong totalCounter = new AtomicLong();
    private final Map<String, AtomicLong> perApiCounter = new ConcurrentHashMap<>();

    public long getTotalCount() {
        return totalCounter.get();
    }

    public Map<String, Long> getPerApiCount() {
        return perApiCounter.entrySet().stream()
                .collect(ConcurrentHashMap::new,
                        (m, e) -> m.put(e.getKey(), e.getValue().get()),
                        ConcurrentHashMap::putAll);
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
            throws IOException, ServletException {
        if (req instanceof HttpServletRequest httpReq) {
            String path = httpReq.getRequestURI();
            totalCounter.incrementAndGet();
            perApiCounter.computeIfAbsent(path, k -> new AtomicLong()).incrementAndGet();
        }
        chain.doFilter(req, res);
    }
}