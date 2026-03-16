# Memory and Segments

## Symptom
With 12 threads indexing 5000 Gutenberg texts (3.7 GB input), Bearing uses far more memory than Java Lucene and produces orders of magnitude more segments. Timing wasn't recorded here, but Rust was slower, which is unusual as well but not unexpected based on what we see here:

| Metric | Java (12T) | Rust (12T) |
|---|---|---|
| Peak RSS | 128 MB | 2.6 GB |
| File count | 119 | ~11,000 |
| Index size | 1.12 GB | 1.53 GB |

## Root Causes
There are multiple root causes going on here that are all interacting with each other to get these results.

### Document Analysis
While analyzing documents, Bearing will read the entire document into memory as a string before parsing/tokenizing. Java, instead, will stream the document contents keeping memory overhead here to a minimum. This is only maybe 5-10% of the problem and easy to fix. It doesn't impact segments generation at all.

### Flushing Approach
Flushing controls how many segments are created. They may later be merged to reduce segments on disk, but our problems start with flushing. Right now we don't do a good job of flushing efficiently. Java will flush the thread that is using the most memory any time the threshold for memory usage is hit. This maximizes segment size and minimizes segment count. There are edge cases with many threads and a small input data set where small segments may be created, but for larger data sets this is approaching ideal and the segment merging cleans up the minor leftovers.

### Segment Merging
As mentioned above, Lucene will merge smaller segments at the tail end of indexing, which also reduces segment count. This requires the ability to read/parse existing file formats which Bearing doesn't support at this time. Adding this support is not until later in the timeline, so we can assume this will not be addressed for a while.

### Accurate Memory Measurement
Lucene has a lot of source code with the goal of accurately measuring how much memory is used by the threads while indexing. Bearing doesn't currently have this. Without this, we'll never have a useful Flushing Approach which is mentioned above. This can aggravate both problems at once. First, it can greatly increase actual memory usage without realizing it. Second, it may cause excessive flushing which in turn generates many small segments.

### Efficient Memory Usage
There are two primary hotspots that have been identified by using Heaptrack. Summarized as follows by Claude: 

"PostingList byte streams (Vec<u8>) growing during tokenization account for ~181M of peak heap. The codec flush path accounts for ~213M, dominated by PositionEncoder::finish() (~81M building position data Vecs) and DataOutput buffer growth inside PostingsWriter::write_term() via pfor_encode (~131M). Both are amplified by concurrent flushes across 12 threads."

At a higher level, there's a lot of overhead on each struct that is repeated for literally thousands of struct instances. Maps that hold these, vecs, etc. all add up to use up memory quickly.

## Suggested Approach
This will still require some research, but here is the higher level approach I'd like to take.

### Document Analyzer
Just switch this to a streaming approach. It's simple and not tied to other issues. It's an easy win.

### Memory Usage Measurement
We must figure out how to accurately measure how much memory each thread is using for the index data its essentially caching until flush. This is where we have to do some research. Rust is a powerful language with better memory access and control than Java. We shouldn't have to resort to Lucene's complexity to achieve this. However, we may have to leveraging external crates and relaxing our dependency policy. Look into bumpalo and arena memory approaches. Also look into whether Rust can accurately and quickly report actual memory usage as it allocates. A trait where each struct can report their usage as they are added to the thread memory may be elegant.

There are libraries that help with this like mem_dbg, deepsize, get-size, etc.

### Flushing Policy
I'd like to start with a reasonably simple approach here. Assume that each thread can accurately measure its memory usage. There is a master thread that manages what documents are handed to which workers. That same master thread can also tell a document when to flush. Each worker thread reports it's current memory usage every time it completes a doc. Once a threshold has been hit, the master thread will select the "fattest" worker thread and ask it to flush before handing it its next document. The logic and coordination should be pretty simple. We should assume something like 80-90% usage of maximum memory as the threshold to give a little room for a thread to complete what it's working on before flushing.

It's important that a thread is properly cleaned up during flush before recieving its next document. Its IndexingChain needs to be reinitialized or replaced with a new one.

### Memory and Struct Optimization
We should consider restructing to reduce memory overhead in some of the Bearing structs on the indexing path. This should start with memory profiling to truly understand the usage, but focus on the index time structures.

Lucene uses an arena approach, as does Tantivy. This actually attacks two problems at once. Accurately measuring usage AND optimizing memory storage at index time. However, this is a fairly large structural change that will permeate the entire indexing side of the codebase. There are libraries we can use that will help, like bumpalo and hashbrown. 


