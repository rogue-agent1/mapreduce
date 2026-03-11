#!/usr/bin/env python3
"""MapReduce framework simulation."""
import sys, collections, re
def map_fn(key,value):
    for word in re.findall(r'\w+',value.lower()): yield word,1
def reduce_fn(key,values): yield key,sum(values)
def mapreduce(data,mapper,reducer,num_mappers=3):
    # Split input
    chunks=[[] for _ in range(num_mappers)]
    for i,(k,v) in enumerate(data): chunks[i%num_mappers].append((k,v))
    # Map phase
    intermediate=collections.defaultdict(list)
    for chunk in chunks:
        for k,v in chunk:
            for mk,mv in mapper(k,v): intermediate[mk].append(mv)
    # Shuffle + Reduce
    results={}
    for k in sorted(intermediate):
        for rk,rv in reducer(k,intermediate[k]): results[rk]=rv
    return results
docs=[('doc1','the cat sat on the mat'),('doc2','the dog sat on the log'),
      ('doc3','the cat and the dog are friends')]
print("MapReduce Word Count:")
results=mapreduce(docs,map_fn,reduce_fn)
for word,count in sorted(results.items(),key=lambda x:-x[1])[:10]:
    print(f"  {word:10s}: {count}")
