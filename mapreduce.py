#!/usr/bin/env python3
"""MapReduce — simplified parallel data processing framework."""
import sys, re
from collections import defaultdict

def word_count_map(chunk):
    return [(w, 1) for w in re.findall(r"\w+", chunk.lower())]

def char_freq_map(chunk):
    return [(c, 1) for c in chunk.lower() if c.isalpha()]

def sum_reduce(key, values):
    return key, sum(values)

def mapreduce(data, map_fn, reduce_fn, chunks=4):
    chunk_size = max(1, len(data) // chunks)
    parts = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
    # Map phase
    mapped = []
    for part in parts:
        mapped.extend(map_fn(part))
    # Shuffle phase
    shuffled = defaultdict(list)
    for k, v in mapped:
        shuffled[k].append(v)
    # Reduce phase
    return dict(reduce_fn(k, vs) for k, vs in shuffled.items())

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] != "-":
        data = open(sys.argv[1]).read()
    elif not sys.stdin.isatty():
        data = sys.stdin.read()
    else:
        data = "The quick brown fox jumps over the lazy dog. The dog barked at the fox."
    print("=== Word Count ===")
    wc = mapreduce(data, word_count_map, sum_reduce)
    for w, c in sorted(wc.items(), key=lambda x: -x[1])[:15]:
        print(f"  {w}: {c}")
    print(f"\n=== Character Frequency ===")
    cf = mapreduce(data, char_freq_map, sum_reduce)
    total = sum(cf.values())
    for c, n in sorted(cf.items(), key=lambda x: -x[1])[:10]:
        bar = "█" * (n * 30 // max(cf.values()))
        print(f"  {c}: {n:4d} ({n/total:5.1%}) {bar}")
