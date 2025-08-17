package com.example.bulkloader;

import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.error.AmbiguousTimeoutException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.codec.JsonTranscoder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.UpsertOptions;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class BulkLoader {
    public static void main(String[] args) {
        String hostsEnv = getenv("CB_HOSTS", String.join(",",
                Arrays.asList(
                        "ec2-15-237-220-120.eu-west-3.compute.amazonaws.com",
                        "ec2-15-237-93-147.eu-west-3.compute.amazonaws.com",
                        "ec2-35-181-7-225.eu-west-3.compute.amazonaws.com"
                )));
        String username = getenv("CB_USER", "Administrator");
        String password = getenv("CB_PASS", "password");
        String bucketName = getenv("CB_BUCKET", "default");
        long docCount = parseLongEnv("DOC_COUNT", 100_000_000L);
        int docSize = parseIntEnv("DOC_SIZE", 800);
        int concurrency = parseIntEnv("CONCURRENCY", 8192);
        int numKvConnections = parseIntEnv("KV_CONNECTIONS", 16);
        int eventLoopThreads = parseIntEnv("IO_THREADS", Runtime.getRuntime().availableProcessors());
        long targetRps = parseLongEnv("TARGET_RPS", 0L); // 0 disables pacing
        int maxRetries = parseIntEnv("RETRY_MAX_ATTEMPTS", 5);
        long baseBackoffMs = parseLongEnv("RETRY_BACKOFF_MS", 10L);
        long maxBackoffMs = parseLongEnv("RETRY_BACKOFF_MAX_MS", 1000L);
        boolean compressionEnabled = parseBooleanEnv("COMPRESSION_ENABLED", false);
        DurabilityLevel durabilityLevel = parseDurability(getenv("DURABILITY", "PERSIST_TO_MAJORITY"));
        String keyPrefix = getenv("KEY_PREFIX", "doc::");

        String connectionString = "couchbase://" + Arrays.stream(hostsEnv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining(","));

        System.out.println("Connecting to: " + connectionString + ", durability=" + durabilityLevel + ", compression=" + compressionEnabled);
        ClusterEnvironment env = ClusterEnvironment.builder()
                .ioConfig(IoConfig.numKvConnections(numKvConnections))
                .compressionConfig(CompressionConfig.enable(compressionEnabled))
                .timeoutConfig(TimeoutConfig
                        .kvTimeout(Duration.ofSeconds(15))
                        .connectTimeout(Duration.ofSeconds(15)))
                .build();

        Cluster cluster = Cluster.connect(connectionString, ClusterOptions.clusterOptions(username, password).environment(env));
        Bucket bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.ofSeconds(30));
        Collection collection = bucket.defaultCollection();
        ReactiveCollection rc = collection.reactive();

        ObjectMapper mapper = new ObjectMapper();
        
        UpsertOptions upsertOptions = UpsertOptions.upsertOptions()
                .durability(durabilityLevel);

        Retry retrySpec = Retry
                .backoff(maxRetries, Duration.ofMillis(baseBackoffMs))
                .maxBackoff(Duration.ofMillis(maxBackoffMs))
                .jitter(0.5)
                .filter(BulkLoader::isRetryable);

        AtomicLong success = new AtomicLong();
        AtomicLong errors = new AtomicLong();
        long startNs = System.nanoTime();
        final long nanosPerDoc = targetRps > 0 ? (long) (1_000_000_000.0 / (double) targetRps) : 0L;

        Disposable progress = Flux.interval(Duration.ofSeconds(1))
                .doOnNext(tick -> {
                    long s = success.get();
                    long e = errors.get();
                    double elapsed = (System.nanoTime() - startNs) / 1_000_000_000.0;
                    double rate = elapsed > 0 ? s / elapsed : 0.0;
                    System.out.printf("Progress: written %,d docs, errors %,d, rate ~%,.0f docs/s\n", s, e, rate);
                })
                .subscribe();

        try {
            Flux<Integer> ids = Flux.range(0, (int) docCount);

            ids.flatMap(i -> {
                        String key = keyPrefix + i;
                        Mono<Long> gate = nanosPerDoc > 0
                                ? Mono.defer(() -> {
                                    long scheduledNs = startNs + (long) i * nanosPerDoc;
                                    long delayNs = Math.max(0L, scheduledNs - System.nanoTime());
                                    return delayNs > 0 ? Mono.delay(Duration.ofNanos(delayNs)).thenReturn(0L) : Mono.just(0L);
                                })
                                : Mono.just(0L);

                        ObjectNode client = createFakeClient(mapper, docSize);
                        Mono<MutationResult> op = gate.then(rc.upsert(key, client, upsertOptions)
                                .retryWhen(retrySpec));

                        return op.doOnSuccess(res -> success.incrementAndGet())
                                .onErrorResume(err -> {
                                    errors.incrementAndGet();
                                    return Mono.delay(Duration.ofMillis(50)).then(Mono.empty());
                                });
                    },
                    concurrency,
                    concurrency
            )
            .then()
            .block();
        } finally {
            progress.dispose();
            cluster.disconnect();
            env.shutdown();
            long total = success.get() + errors.get();
            double elapsed = (System.nanoTime() - startNs) / 1_000_000_000.0;
            double rate = elapsed > 0 ? success.get() / elapsed : 0.0;
            System.out.printf("Done: attempted %,d, success %,d, errors %,d, avg rate ~%,.0f docs/s\n", total, success.get(), errors.get(), rate);
        }
    }

    private static boolean isRetryable(Throwable t) {
        return t instanceof TemporaryFailureException
                || t instanceof RequestCanceledException
                || t instanceof UnambiguousTimeoutException
                || t instanceof AmbiguousTimeoutException;
    }

    private static DurabilityLevel parseDurability(String s) {
        try {
            return DurabilityLevel.valueOf(s.trim().toUpperCase());
        } catch (Exception e) {
            return DurabilityLevel.NONE;
        }
    }

    private static String getenv(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isEmpty() ? def : v;
    }
    
    private static ObjectNode createFakeClient(ObjectMapper mapper, int targetSize) {
        ObjectNode client = mapper.createObjectNode();
        
        // Base client data
        client.put("id", ThreadLocalRandom.current().nextLong(1000000, 9999999));
        client.put("firstName", generateRandomName());
        client.put("lastName", generateRandomName());
        client.put("email", generateRandomEmail());
        client.put("phone", generateRandomPhone());
        client.put("dateOfBirth", generateRandomDate());
        client.put("address", generateRandomAddress());
        client.put("city", generateRandomCity());
        client.put("postalCode", String.valueOf(ThreadLocalRandom.current().nextInt(10000, 99999)));
        client.put("country", "France");
        client.put("registrationDate", generateRandomDate());
        client.put("lastLogin", generateRandomDate());
        client.put("status", ThreadLocalRandom.current().nextBoolean() ? "active" : "inactive");
        client.put("premium", ThreadLocalRandom.current().nextBoolean());
        client.put("points", ThreadLocalRandom.current().nextInt(0, 10000));
        
        // Add random fields to reach target size
        String currentJson = client.toString();
        while (currentJson.length() < targetSize) {
            String fieldName = "field" + ThreadLocalRandom.current().nextInt(1000, 9999);
            String fieldValue = generateRandomString(targetSize - currentJson.length() - 20); // Leave room for field name and quotes
            client.put(fieldName, fieldValue);
            currentJson = client.toString();
        }
        
        return client;
    }
    
    private static String generateRandomName() {
        String[] names = {"Jean", "Marie", "Pierre", "Sophie", "Michel", "Isabelle", "François", "Catherine", "Philippe", "Nathalie"};
        return names[ThreadLocalRandom.current().nextInt(names.length)];
    }
    
    private static String generateRandomEmail() {
        String[] domains = {"gmail.com", "yahoo.fr", "hotmail.com", "outlook.com", "orange.fr"};
        String name = generateRandomName().toLowerCase();
        String domain = domains[ThreadLocalRandom.current().nextInt(domains.length)];
        return name + ThreadLocalRandom.current().nextInt(100, 999) + "@" + domain;
    }
    
    private static String generateRandomPhone() {
        return "0" + ThreadLocalRandom.current().nextInt(1, 9) + ThreadLocalRandom.current().nextInt(10000000, 99999999);
    }
    
    private static String generateRandomDate() {
        int year = ThreadLocalRandom.current().nextInt(1950, 2005);
        int month = ThreadLocalRandom.current().nextInt(1, 13);
        int day = ThreadLocalRandom.current().nextInt(1, 29);
        return String.format("%04d-%02d-%02d", year, month, day);
    }
    
    private static String generateRandomAddress() {
        String[] streets = {"Rue de la Paix", "Avenue des Champs", "Boulevard Saint-Michel", "Place de la République", "Rue du Commerce"};
        return streets[ThreadLocalRandom.current().nextInt(streets.length)] + " " + ThreadLocalRandom.current().nextInt(1, 200);
    }
    
    private static String generateRandomCity() {
        String[] cities = {"Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille"};
        return cities[ThreadLocalRandom.current().nextInt(cities.length)];
    }
    
    private static String generateRandomString(int maxLength) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder();
        int length = Math.max(10, Math.min(maxLength, 100));
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(ThreadLocalRandom.current().nextInt(chars.length())));
        }
        return sb.toString();
    }

    private static int parseIntEnv(String key, int def) {
        String v = System.getenv(key);
        if (v == null || v.isEmpty()) return def;
        return Integer.parseInt(v);
        
    }

    private static long parseLongEnv(String key, long def) {
        String v = System.getenv(key);
        if (v == null || v.isEmpty()) return def;
        return Long.parseLong(v);
    }

    private static boolean parseBooleanEnv(String key, boolean def) {
        String v = System.getenv(key);
        if (v == null || v.isEmpty()) return def;
        String s = v.trim().toLowerCase();
        return s.equals("1") || s.equals("true") || s.equals("yes");
    }
} 