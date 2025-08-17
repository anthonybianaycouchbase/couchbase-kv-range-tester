package com.example.bulkloader;

import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.Map;

public class KeyRangeReader {
	
	public static void main(String[] args) {
		String mode = getenv("MODE", "SINGLE");
		
		if ("PERFORMANCE".equals(mode)) {
			runPerformanceBenchmark();
		} else {
			runSingleRangeRead();
		}
	}
	
	public static void runPerformanceBenchmark() {
		System.out.println("=== KV Range Performance Benchmark (High Concurrency) ===\n");
		
		String hostsEnv = getenv("CB_HOSTS", "ec2-15-237-220-120.eu-west-3.compute.amazonaws.com,ec2-15-237-93-147.eu-west-3.compute.amazonaws.com,ec2-35-181-7-225.eu-west-3.compute.amazonaws.com");
		String username = getenv("CB_USER", "Administrator");
		String password = getenv("CB_PASS", "password");
		String bucketName = getenv("CB_BUCKET", "KVRANGE");
		String keyPrefix = getenv("KEY_PREFIX", "client::");
		
		// Benchmark scenarios with High Concurrency only
		List<BenchmarkScenario> scenarios = Arrays.asList(
			// Small ranges (1K docs) - High Concurrency
			new BenchmarkScenario("Small Range (1K)", 1000, 8192, 32),
			
			// Medium ranges (10K docs) - High Concurrency
			new BenchmarkScenario("Medium Range (10K)", 10000, 8192, 32),
			
			// Large ranges (100K docs) - High Concurrency
			new BenchmarkScenario("Large Range (100K)", 100000, 8192, 32),
			
			// Very large ranges (1M docs) - High Concurrency
			new BenchmarkScenario("Very Large Range (1M)", 1000000, 8192, 32)
		);
		
		// Run all scenarios
		for (BenchmarkScenario scenario : scenarios) {
			runScenario(hostsEnv, username, password, bucketName, keyPrefix, scenario);
			System.out.println(); // Empty line between scenarios
		}
		
		// Summary table
		printSummaryTable(scenarios);
		
		// Time per document analysis
		printTimePerDocumentAnalysis(scenarios);
	}
	
	public static void runScenario(String hostsEnv, String username, String password, 
								  String bucketName, String keyPrefix, BenchmarkScenario scenario) {
		System.out.printf("Running: %s\n", scenario.name);
		System.out.printf("  Range size: %,d documents\n", scenario.rangeSize);
		System.out.printf("  Concurrency: %,d\n", scenario.concurrency);
		System.out.printf("  KV Connections: %d\n", scenario.kvConnections);
		
		// Use a fixed starting point to avoid conflicts
		int startKey = 1000000; // Start from 1M to avoid conflicts with existing data
		int endKey = startKey + scenario.rangeSize - 1;
		
		long startTime = System.nanoTime();
		PerformanceResult result = measureRangePerformance(
			hostsEnv, username, password, bucketName, keyPrefix,
			startKey, endKey, scenario.concurrency, scenario.kvConnections
		);
		long endTime = System.nanoTime();
		
		scenario.result = result;
		scenario.actualDuration = (endTime - startTime) / 1_000_000_000.0;
		
		System.out.printf("  Results: %,d docs in %.3fs (~%,.0f docs/s)\n", 
			result.docCount, scenario.actualDuration, result.docsPerSecond);
		System.out.printf("  Errors: %d (%.2f%%)\n", 
			result.errorCount, result.errorCount > 0 ? (result.errorCount * 100.0 / result.totalAttempts) : 0.0);
	}
	
	public static void printSummaryTable(List<BenchmarkScenario> scenarios) {
		System.out.println("=== Performance Summary ===");
		System.out.printf("%-40s %12s %12s %12s %12s %12s\n", 
			"Scenario", "Range Size", "Duration(s)", "Docs/s", "Errors", "Error %");
		System.out.println("-".repeat(100));
		
		for (BenchmarkScenario scenario : scenarios) {
			if (scenario.result != null) {
				double errorPercent = scenario.result.errorCount > 0 ? 
					(scenario.result.errorCount * 100.0 / scenario.result.totalAttempts) : 0.0;
				System.out.printf("%-40s %,12d %12.3f %,12.0f %,12d %11.2f%%\n",
					scenario.name, scenario.rangeSize, scenario.actualDuration, 
					scenario.result.docsPerSecond, scenario.result.errorCount, errorPercent);
			}
		}
	}
	
	public static PerformanceResult measureRangePerformance(
			String hostsCsv, String username, String password, String bucketName,
			String keyPrefix, int startInclusive, int endInclusive,
			int concurrency, int kvConnections) {
		
		String connectionString = "couchbase://" + Arrays.stream(hostsCsv.split(","))
				.map(String::trim)
				.filter(s -> !s.isEmpty())
				.collect(Collectors.joining(","));

		ClusterEnvironment env = ClusterEnvironment.builder()
				.ioConfig(IoConfig.numKvConnections(kvConnections))
				.timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofSeconds(15)))
				.build();

		Cluster cluster = Cluster.connect(connectionString, ClusterOptions.clusterOptions(username, password).environment(env));
		
		try {
			Bucket bucket = cluster.bucket(bucketName);
			bucket.waitUntilReady(Duration.ofSeconds(30));
			Collection collection = bucket.defaultCollection();
			ReactiveCollection rc = collection.reactive();

			int count = endInclusive - startInclusive + 1;
			AtomicInteger fetched = new AtomicInteger(0);
			AtomicInteger errors = new AtomicInteger(0);
			AtomicLong totalAttempts = new AtomicLong(0);
			long startNs = System.nanoTime();

			Flux.range(startInclusive, count)
					.flatMap(n -> {
						totalAttempts.incrementAndGet();
						return rc.get(keyPrefix + n, GetOptions.getOptions().transcoder(RawBinaryTranscoder.INSTANCE))
								.doOnNext(gr -> fetched.incrementAndGet())
								.onErrorResume(e -> {
									errors.incrementAndGet();
									return Mono.empty();
								});
					},
					concurrency,
					concurrency
				)
				.blockLast();

			long endNs = System.nanoTime();
			double duration = (endNs - startNs) / 1_000_000_000.0;
			double docsPerSecond = duration > 0 ? fetched.get() / duration : 0.0;
			
			return new PerformanceResult(fetched.get(), errors.get(), totalAttempts.get(), docsPerSecond, duration);
			
		} finally {
			cluster.disconnect();
			env.shutdown();
		}
	}

	public static void runSingleRangeRead() {
		String hostsEnv = getenv("CB_HOSTS", "ec2-15-237-220-120.eu-west-3.compute.amazonaws.com,ec2-15-237-93-147.eu-west-3.compute.amazonaws.com,ec2-35-181-7-225.eu-west-3.compute.amazonaws.com");
		String username = getenv("CB_USER", "Administrator");
		String password = getenv("CB_PASS", "password");
		String bucketName = getenv("CB_BUCKET", "KVRANGE");
		String scopeName = getenv("CB_SCOPE", "_default");
		String collectionName = getenv("CB_COLLECTION", "_default");

		// Two ways to define the range:
		// 1) Explicit keys (recommended): START_KEY, END_KEY (e.g., b00::10000, b00::11000)
		// 2) Numeric range with common KEY_PREFIX: KEY_PREFIX, START, END
		String startKeyEnv = getenv("START_KEY", "");
		String endKeyEnv = getenv("END_KEY", "");
		String prefix;
		int start;
		int endInclusive;
		if (!startKeyEnv.isEmpty() && !endKeyEnv.isEmpty()) {
			ParsedKey sk = parseKey(startKeyEnv);
			ParsedKey ek = parseKey(endKeyEnv);
			if (!sk.prefix.equals(ek.prefix)) {
				throw new IllegalArgumentException("START_KEY and END_KEY must share the same non-numeric prefix");
			}
			prefix = sk.prefix;
			start = sk.number;
			endInclusive = ek.number;
		} else {
			prefix = getenv("KEY_PREFIX", "client::");
			start = parseIntEnv("START", 10000);
			endInclusive = parseIntEnv("END", 11000);
		}

		int concurrency = parseIntEnv("CONCURRENCY", 4096);
		int kvConnections = parseIntEnv("KV_CONNECTIONS", 16);
		int kvTimeoutSeconds = parseIntEnv("KV_TIMEOUT_SECONDS", 5);

		long t0 = System.nanoTime();
		List<GetResult> docs = readRangeByNumericSuffix(
			hostsEnv, username, password, bucketName, scopeName, collectionName,
			prefix, start, endInclusive, concurrency, kvConnections, kvTimeoutSeconds
		);
		double elapsed = (System.nanoTime() - t0) / 1_000_000_000.0;
		double rate = elapsed > 0 ? docs.size() / elapsed : 0.0;
		System.out.printf("Fetched %,d docs in %.3fs (~%,.0f docs/s) from %s%d to %s%d\n",
			docs.size(), elapsed, rate, prefix, start, prefix, endInclusive);
	}

	public static List<GetResult> readRangeByNumericSuffix(
			String hostsCsv,
			String username,
			String password,
			String bucketName,
			String scopeName,
			String collectionName,
			String keyPrefix,
			int startInclusive,
			int endInclusive,
			int concurrency,
			int kvConnections,
			int kvTimeoutSeconds
	) {
		if (endInclusive < startInclusive) {
			return List.of();
		}

		String connectionString = "couchbase://" + Arrays.stream(hostsCsv.split(","))
				.map(String::trim)
				.filter(s -> !s.isEmpty())
				.collect(Collectors.joining(","));

		ClusterEnvironment env = ClusterEnvironment.builder()
				.ioConfig(IoConfig.numKvConnections(kvConnections))
				.timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofSeconds(kvTimeoutSeconds)))
				.build();

		Cluster cluster = Cluster.connect(connectionString, ClusterOptions.clusterOptions(username, password).environment(env));
		try {
			Bucket bucket = cluster.bucket(bucketName);
			bucket.waitUntilReady(Duration.ofSeconds(30));
			Collection collection = bucket.scope(scopeName).collection(collectionName);
			ReactiveCollection rc = collection.reactive();

			int count = endInclusive - startInclusive + 1;
			AtomicInteger fetched = new AtomicInteger(0);
			List<GetResult> out = new ArrayList<>(Math.min(count, 50_000));

			Flux.range(startInclusive, count)
					.flatMap(n -> rc.get(keyPrefix + n, GetOptions.getOptions().transcoder(RawBinaryTranscoder.INSTANCE))
							.doOnNext(gr -> {
								out.add(gr);
								fetched.incrementAndGet();
							})
							.onErrorResume(e -> Mono.empty())
					,
					concurrency,
					concurrency
				)
				.blockLast();

			return out;
		} finally {
			cluster.disconnect();
			env.shutdown();
		}
	}

	// Data classes for performance measurement
	public static class BenchmarkScenario {
		final String name;
		final int rangeSize;
		final int concurrency;
		final int kvConnections;
		PerformanceResult result;
		double actualDuration;
		
		BenchmarkScenario(String name, int rangeSize, int concurrency, int kvConnections) {
			this.name = name;
			this.rangeSize = rangeSize;
			this.concurrency = concurrency;
			this.kvConnections = kvConnections;
		}
	}
	
	public static class PerformanceResult {
		final int docCount;
		final int errorCount;
		final long totalAttempts;
		final double docsPerSecond;
		final double duration;
		
		PerformanceResult(int docCount, int errorCount, long totalAttempts, double docsPerSecond, double duration) {
			this.docCount = docCount;
			this.errorCount = errorCount;
			this.totalAttempts = totalAttempts;
			this.docsPerSecond = docsPerSecond;
			this.duration = duration;
		}
	}

	private static class ParsedKey {
		final String prefix;
		final int number;
		ParsedKey(String p, int n) { this.prefix = p; this.number = n; }
	}

	private static ParsedKey parseKey(String key) {
		int i = key.length() - 1;
		while (i >= 0 && Character.isDigit(key.charAt(i))) {
			i--;
		}
		if (i < 0 || i == key.length() - 1) {
			throw new IllegalArgumentException("Key must end with a numeric suffix: " + key);
		}
		String prefix = key.substring(0, i + 1);
		int num = Integer.parseInt(key.substring(i + 1));
		return new ParsedKey(prefix, num);
	}

	private static String getenv(String key, String def) {
		String v = System.getenv(key);
		return v == null || v.isEmpty() ? def : v;
	}

	private static int parseIntEnv(String key, int def) {
		String v = System.getenv(key);
		if (v == null || v.isEmpty()) return def;
		return Integer.parseInt(v);
	}

	public static void printTimePerDocumentAnalysis(List<BenchmarkScenario> scenarios) {
		System.out.println("=== Temps par Document - Analyse (High Concurrency) ===\n");
		System.out.printf("%-40s %12s %15s %15s %15s\n", 
			"Scenario", "Documents", "Temps Total(s)", "Temps/Doc(ms)", "Temps/Doc(μs)");
		System.out.println("-".repeat(100));
		
		for (BenchmarkScenario scenario : scenarios) {
			if (scenario.result != null) {
				double timePerDocMs = (scenario.actualDuration * 1000.0) / scenario.rangeSize;
				double timePerDocUs = (scenario.actualDuration * 1000000.0) / scenario.rangeSize;
				
				System.out.printf("%-40s %,12d %15.3f %15.3f %15.1f\n",
					scenario.name, scenario.rangeSize, scenario.actualDuration, timePerDocMs, timePerDocUs);
			}
		}
		
		System.out.println("\n=== Résumé des Performances (High Concurrency) ===\n");
		
		// Find best performer
		BenchmarkScenario best = null;
		double bestTime = Double.MAX_VALUE;
		
		for (BenchmarkScenario scenario : scenarios) {
			if (scenario.result != null) {
				double timePerDoc = scenario.actualDuration / scenario.rangeSize;
				
				if (timePerDoc < bestTime) {
					bestTime = timePerDoc;
					best = scenario;
				}
			}
		}
		
		if (best != null) {
			double timePerDocMs = (best.actualDuration * 1000.0) / best.rangeSize;
			System.out.printf("🏆 Meilleur temps par document: %s\n", best.name);
			System.out.printf("   Temps par document: %.3f ms (%.1f μs)\n", timePerDocMs, timePerDocMs * 1000);
			System.out.printf("   Configuration: %d ops, %d connexions (High Concurrency)\n", best.concurrency, best.kvConnections);
		}
		
		// Scaling analysis
		System.out.println("\n=== Analyse de l'Évolutivité (High Concurrency) ===\n");
		
		// Sort scenarios by range size
		List<BenchmarkScenario> sortedScenarios = scenarios.stream()
			.filter(s -> s.result != null)
			.sorted((a, b) -> Integer.compare(a.rangeSize, b.rangeSize))
			.collect(Collectors.toList());
		
		for (BenchmarkScenario scenario : sortedScenarios) {
			double timePerDocMs = (scenario.actualDuration * 1000.0) / scenario.rangeSize;
			
			System.out.printf("📊 Range %,d documents (High Concurrency):\n", scenario.rangeSize);
			System.out.printf("   Temps par document: %.3f ms (%.1f μs)\n", timePerDocMs, timePerDocMs * 1000);
			System.out.printf("   Débit: %,.0f docs/s\n", scenario.result.docsPerSecond);
			System.out.println();
		}
		
		// Time estimation formula
		System.out.println("=== Estimation de Temps pour N Documents ===\n");
		System.out.println("Configuration: High Concurrency (8,192 ops, 32 connexions)");
		System.out.println("Pour estimer le temps d'un range de N documents :");
		System.out.println("• Petits ranges (< 10K): ~1.9 ms par document");
		System.out.println("• Ranges moyens (10K-100K): ~0.1 ms par document");
		System.out.println("• Grands ranges (> 100K): ~0.01 ms par document");
		System.out.println();
		System.out.println("Formule: Temps = Nombre de Documents × Temps par Document");
	}
} 