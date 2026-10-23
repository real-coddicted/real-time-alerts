package io.alerting.rules;

import io.alerting.json.Json;
import io.alerting.json.JsonAccess;
import io.alerting.model.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fetches all rules from a REST endpoint returning JSON {@code {"rules": [...], "next_token": "..."}},
 * following {@code next_token} pagination ({@code page_size}, {@code page_token} query params).
 * Retries on I/O errors and HTTP 429/502/503/504 with exponential backoff (spec 4.2 defaults).
 */
public final class HttpRuleProvider implements RuleProvider {
    private static final Logger LOG = LoggerFactory.getLogger(HttpRuleProvider.class);
    private static final Set<Integer> RETRYABLE = Set.of(429, 502, 503, 504);

    private final URI uri;
    private final Map<String, String> headers;
    private final int pageSize;
    private final Duration readTimeout;
    private final int maxAttempts;
    private final Duration initialBackoff;
    private final Duration maxBackoff;
    private final HttpClient client;

    public HttpRuleProvider(URI uri, Map<String, String> headers, int pageSize, Duration connectTimeout,
                            Duration readTimeout, int maxAttempts, Duration initialBackoff, Duration maxBackoff) {
        this.uri = uri;
        this.headers = Map.copyOf(headers);
        this.pageSize = pageSize;
        this.readTimeout = readTimeout;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
        this.client = HttpClient.newBuilder().connectTimeout(connectTimeout).build();
    }

    @Override
    public List<Rule> fetchRules() throws Exception {
        List<Rule> all = new ArrayList<>();
        String token = "";
        do {
            StringBuilder q = new StringBuilder(uri.toString())
                    .append(uri.getQuery() == null ? '?' : '&')
                    .append("page_size=").append(pageSize);
            if (!token.isEmpty()) q.append("&page_token=").append(URLEncoder.encode(token, StandardCharsets.UTF_8));
            Map<String, Object> page = JsonAccess.asMap(Json.parse(get(URI.create(q.toString()))), "rules response");
            all.addAll(RulesParser.parse(page));
            token = JsonAccess.getString(page, "next_token", "");
        } while (!token.isEmpty());
        return all;
    }

    private String get(URI target) throws IOException, InterruptedException {
        HttpRequest.Builder rb = HttpRequest.newBuilder(target).timeout(readTimeout).header("Accept", "application/json").GET();
        headers.forEach(rb::header);
        HttpRequest request = rb.build();
        long backoff = initialBackoff.toMillis();
        for (int attempt = 1; ; attempt++) {
            String failure;
            try {
                HttpResponse<String> resp = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() / 100 == 2) return resp.body();
                failure = "HTTP " + resp.statusCode();
                if (!RETRYABLE.contains(resp.statusCode())) throw new NonRetryable("rules API returned " + failure);
            } catch (NonRetryable e) {
                throw e;
            } catch (IOException e) {
                failure = e.toString();
            }
            if (attempt >= maxAttempts) throw new IOException("rules API failed after " + attempt + " attempts: " + failure);
            LOG.warn("rules API attempt {} failed ({}), retrying in {}ms", attempt, failure, backoff);
            Thread.sleep(backoff);
            backoff = Math.min(maxBackoff.toMillis(), (long) (backoff * 1.5));
        }
    }

    private static final class NonRetryable extends IOException {
        private static final long serialVersionUID = 1L;

        NonRetryable(String message) {
            super(message);
        }
    }

    @Override
    public String toString() {
        return "http:" + uri;
    }
}
