"""Run a matched Redis/MiniRedis benchmark against two disposable loopback servers."""
import argparse
import csv
from collections import defaultdict
import math
import statistics
import subprocess
import sys
import uuid


def run(program, port, *args):
    return subprocess.run(
        [program, '-h', '127.0.0.1', '-p', str(port), *args],
        check=True, stdout=subprocess.PIPE, text=True, timeout=180,
    ).stdout


def smoke(port):
    key = 'day14:' + uuid.uuid4().hex
    try:
        for args, expected in [
            (('PING',), 'PONG'),
            (('SET', key, 'hello'), 'OK'),
            (('GET', key), 'hello'),
        ]:
            actual = run('redis-cli', port, '--raw', *args).strip()
            if actual != expected:
                raise RuntimeError(f'Port {port}: {args[0]} returned {actual!r}')
    finally:
        run('redis-cli', port, '--raw', 'DEL', key)


def measure(port, pipeline, requests):
    output = run('redis-benchmark', port, '--csv', '-c', '50', '-n',
                 str(requests), '-d', '64', '-P', str(pipeline), '-t', 'set,get')
    rows = {}
    for row in csv.DictReader(output.splitlines()):
        name = row['test'].upper()
        if name not in ('SET', 'GET') or name in rows:
            raise RuntimeError(f'Unexpected benchmark row: {row}')
        numbers = tuple(float(row[field]) for field in
                        ('rps', 'p50_latency_ms', 'p95_latency_ms'))
        if not all(math.isfinite(n) and n >= 0 for n in numbers) or numbers[0] <= 0:
            raise RuntimeError(f'Invalid benchmark measurements: {row}')
        rows[name] = numbers
    if set(rows) != {'SET', 'GET'}:
        raise RuntimeError(f'Port {port}: missing SET or GET benchmark output')
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--smoke-only', action='store_true')
    args = parser.parse_args()
    servers = [('MiniRedis', 6380), ('Redis', 6399)]
    for name, port in servers:
        smoke(port)
        print(f'PASS {name} on {port}: PING, SET, GET and cleanup', file=sys.stderr)
    if args.smoke_only:
        return
    for _, port in servers:
        for pipeline in (1, 16):
            measure(port, pipeline, 10000)

    writer = csv.writer(sys.stdout, lineterminator='\n')
    writer.writerow(('server', 'pipeline', 'command', 'run', 'ops_per_sec', 'p50_ms', 'p95_ms'))
    samples = defaultdict(list)
    for round_number in range(1, 4):
        order = servers if round_number % 2 else servers[::-1]
        for pipeline in (1, 16):
            for name, port in order:
                for command, values in measure(port, pipeline, 100000).items():
                    key = (name, pipeline, command)
                    samples[key].append(values)
                    writer.writerow((*key, round_number, *values))
                    sys.stdout.flush()
    for key, runs in sorted(samples.items()):
        writer.writerow((*key, 'median', *(statistics.median(column) for column in zip(*runs))))


if __name__ == '__main__':
    main()
