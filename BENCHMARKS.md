# MiniRedis benchmark notes

## Historical measurements

These are local historical observations, with the client in WSL and MiniRedis on Windows. They are separate from the Day 14 comparison.

- Day 7, AOF always, 50 clients: about **1,650 SET/s**, p50 **28.335 ms**, 100,000 requests.
- Day 10, AOF always, 50 clients: **19,066 SET/s**, p50 **2.495 ms**, 20,000 requests. After the single-client warm-up, 20,000 additional records shared 437 fsyncs.
- Day 10, pipeline 16, 50 clients: **95,238 SET/s**, p50 **6.695 ms**, 50,000 requests, AOF always.
- Day 10 restart check: the recovered counter matched all **119,423** acknowledged increments. The stop method in that run was not established as a hard kill.

The Day 7 and Day 10 runs used different request counts and included other implementation changes. Treat them as historical observations.

## Day 14 comparison

Use two disposable servers on the same WSL instance, both on loopback, with persistence disabled. Keep them idle except for the benchmark. Install or have available Java 21, Python 3, redis-server, redis-cli and redis-benchmark.

Build MiniRedis in IntelliJ on Windows first. In WSL, use a Java 21 runtime and run the compiled classes from the Windows checkout:

```bash
cd "/mnt/c/Users/Laptop World/Desktop/MiniRedis"
java -cp target/classes com.example.miniredis.Main 6380 none
```

In a second WSL terminal, start a separate Redis instance:

```bash
redis-server --bind 127.0.0.1 --port 6399 --save "" --appendonly no
```

In a third WSL terminal:

```bash
cd "/mnt/c/Users/Laptop World/Desktop/MiniRedis"
python3 scripts/day14_benchmark.py --smoke-only
python3 scripts/day14_benchmark.py | tee day14-results.csv
```

The smoke check verifies PING, SET, GET and DEL against both servers. The benchmark performs a warm-up, then runs three measured rounds with 50 clients, 100,000 requests per command and 64-byte values. SET and GET are measured separately at pipeline depths 1 and 16. It alternates server order and reports each run plus the median throughput and p50/p95 latency. Rows marked median contain the median of the three corresponding run statistics.

Use dedicated servers. redis-benchmark uses its own workload keys, which can overwrite existing data. MiniRedis may emit a CONFIG warning because it implements a subset of Redis. The script requires actual CSV results from each run.

Record the machine, OS, Java and Redis versions with the output. Replace estimated Redis comparisons in the CV only after collecting these results. AOF-disabled throughput measures command handling under these settings and does not measure write durability.

## Day 14 measured results (2026-11-13)

Ubuntu WSL2 loopback, Intel i7-1185G7, OpenJDK 21.0.13.0.3, Redis 8.0.5. Both servers ran in memory with persistence disabled. Three measured runs per configuration, 50 clients, 100,000 requests per command per run, 64-byte values.

| Pipeline | Command | MiniRedis ops/s | Redis ops/s | MiniRedis p95 (ms) | Redis p95 (ms) |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | SET | 74,516 | 61,050 | 0.943 | 0.991 |
| 1 | GET | 71,942 | 77,821 | 0.807 | 0.775 |
| 16 | SET | 598,802 | 564,972 | 3.239 | 2.455 |
| 16 | GET | 719,424 | 781,250 | 2.239 | 1.751 |

These medians describe this workload on one machine. Results varied across runs. See [the raw measurements](benchmarks/day14-wsl.csv) for the individual samples and p50 values.
