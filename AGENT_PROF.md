Prompt: Arrow-C++ deep study for parallel hash join + groupby design (for Golars)

You are a performance engineer. Your job is to study Apache Arrow C++ (arrow-cpp) implementation details and map them into a design for a Go-based dataframe engine (Golars) that already uses arrow-go for columnar memory.

Objective

Do a deep, code-anchored study of Arrow C++’s approaches to:

parallelism / task scheduling

hash join and groupby (including multi-key)

partitioning (radix/hash) and spilling concepts (even if we won’t implement spilling yet)

typed key encoding, dictionary encoding, null semantics

kernel fusion / operator pipelines (Acero execution engine)

Then write a concept → implementation mapping for Golars.

Constraints (non-negotiable)

Do not reinvent algorithms from scratch. Prefer Arrow C++ patterns unless clearly mismatched for Go.

Use primary sources: Arrow C++ docs, design docs, and source code references (file paths, classes, functions).

When you make a claim about how Arrow C++ works, you must attach a citation (link) and, if possible, a file/function name.

Avoid blog posts unless they link to upstream docs/code.

Focus on what matters for performance: memory movement, allocations, cache locality, branchiness, scheduler overhead.

Context (Golars current state)

Golars now runs filter/join/groupby through arrow-go.

Multi-key join now uses typed key normalization:

dictionary/string/int64 → shared uint32 ids

packed 2-key uint64 fast path

hashed path for >2 keys

Benchmarks: small joins competitive; medium-safe inner and multikey still behind Polars; Q1 gap ~13× on 250k rows.

We suspect missing Arrow-C++/Polars-grade techniques: partitioned hash join, better groupby hashing, streaming aggregation, operator fusion, smarter parallel scheduling.

Tasks

Identify the relevant Arrow C++ subsystems

Acero engine (execution plan / operators)

compute kernels (filter, take, hash, sort)

join implementation details (hash join operator)

groupby/aggregate implementation details

threading / task scheduler model used by Arrow C++

Extract concrete patterns
For each topic below, write:

“What Arrow C++ does” (with code pointers)

“Why it helps performance”

“How to map it to Go + arrow-go”

“Minimal viable version we should implement first”

Topics:

Partitioned hash join (radix/hash partitioning, per-partition hash tables, probe locality)

Multi-key encoding (fixed-width packing, dictionary encoding, handling nulls)

Hash table design (open addressing vs chaining, load factor strategies)

Parallel execution model (work stealing? thread pool? task groups? grain size)

Groupby with low-cardinality keys (fast paths / small maps / preallocated accumulators)

Aggregation fusion (multiple aggs in one pass; avoiding repeated scans)

Sorting approach and comparator specialization (if relevant in Arrow C++)

Buffer allocation strategies (arena / memory pool behavior and how it affects perf)

Make a Golars design proposal (v2 engine primitives)
Output a short design that includes:

operator pipeline shape for Q6 and Q1

join path for medium-safe inner + multikey

groupby path for Q1-style tiny cardinality

explicit parallel scheduling plan (W workers, partitioning strategy, avoiding channels in hot loops)

what gets cached (dictionary encodings, key packs) and where

Provide a prioritized implementation plan

3–5 milestones, each with:

the change

expected benchmark impact (which benchmarks move)

required receipts (pprof, allocs/op, tables)

risks and failure modes

Output format (must follow)

Arrow C++ Reading Map

Bullet list of the key docs/pages + key source directories/files

Concept Map Table

A table: Arrow C++ concept | Where in code | Why it matters | Golars mapping | MVP approach

Recommendations

5–10 specific actionable changes to Golars

Milestone Plan

A sequenced list of milestones with acceptance criteria tied to our benchmarks

Benchmark targets to keep in mind

H2OAI joins medium-safe:

InnerJoin: ~6.3× vs Polars → aim ≤ 4×

MultiKeyJoin: ~5.6× → aim ≤ 3.5×

TPC-H Q1 (250k rows): ~13× → aim ≤ 6× quickly

Tooling instruction

You must browse the web. Prefer Arrow official docs and GitHub source. Include citations.

Optional “extra forcing function” line (highly recommended)

Add this at the end if the agent tends to hand-wave:

If you cannot cite an upstream Arrow C++ source code location for a claim about join/groupby/parallelism, label the claim as speculation and do not base recommendations on it.
