# Flush Control

## Problem

Bearing doesn't flush efficiently. Java Lucene flushes the thread using the most memory whenever the RAM buffer threshold is hit, maximizing segment size and minimizing segment count. Bearing's current approach produces far more segments than necessary.

Additionally, when worker threads flush, they flush a copy of their work to memory rather than writing directly to disk. This means we keep a copy of essentially all the output index in memory while indexing. Lucene writes all the way out to disk to avoid this.

## Flushing Policy

Start with a reasonably simple approach. Assume that each thread can accurately measure its memory usage. There is a master thread that manages what documents are handed to which workers. That same master thread can also tell a worker when to flush. Each worker thread reports its current memory usage every time it completes a doc. Once a threshold has been hit, the master thread will select the "fattest" worker thread and ask it to flush before handing it its next document. The logic and coordination should be pretty simple. We should assume something like 80-90% usage of maximum memory as the threshold to give a little room for a thread to complete what it's working on before flushing.

It's important that a thread is properly cleaned up during flush before receiving its next document. Its IndexingChain needs to be reinitialized or replaced with a new one.

To note, this doesn't currently work well with the Gutenberg documents. Each Gutenberg document takes up about 1-2MB of memory for the threads, so they end up flushing every 1-2 documents with this strategy. This may be a strong indication that the memory and struct optimization needs to be completed before we see the benefit.

## Flush to Disk, not Memory

We should implement a similar approach as Lucene in that flushing from the workers should go all the way to disk, not make a memory copy.

We need to be sure this keeps functioning both with and without Compound files. We also need to be sure this doesn't block threads for too long when they flush.

## Caveat

The original analysis was against Gutenberg samples where each document is an entire book. While this highlighted inefficiencies, it may not be the best example to focus efforts on. Regardless, the benefits do show on smaller documents as well.
