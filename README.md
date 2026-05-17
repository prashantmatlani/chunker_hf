---
title: Chunker
emoji: 🐨
colorFrom: yellow
colorTo: gray
sdk: docker
pinned: false
short_description: Chunker simple from Sergii Makarevych's SDDW-based Chunker
---

# Chunker

[Chunker] [https://huggingface.co/spaces/prashantmatlani/chunker], a supposed substitute for RAG, is a low-level "browse a file and extract" tool of knowledge extraction via Progressive Disclosure from a large and loaded source such as a book (articles, papers work too) - taking a cue from Sergii Makarevych's [work] [https://www.linkedin.com/pulse/new-way-encode-documents-ai-agents-navigable-trees-sergii-makarevych-a6cof/], in turn derived from Andrej Karpathy's [LLM Wiki pattern] [https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f]

Chunker fulfills the base requirement of getting something along the lines of chunks and summaries as elaborated paragraphs from knowledge-source such as books. - though scaled up a little to include expanded Summary at Level 3 - worth hundreds of pages; called, although, "Jungian Chunker" it's usable for any pdf (though yet to be checked the max number of pages it can cover upto, but it probably runs into a few hundreds, and on the downside, its time-consuming)

The combined script - with two Phases, I (Extract and rewrite chunks - The "Leaves") and II (Incremental Aggregation into Summaries - The "Branches"), fired sequentially - aligns with: 

* the "Dense Theory" of knowledge extraction, and 
* with Makarevych's "Incremental Aggregation" logic of the availability of a set of chunks triggering the system to generate a summary. 

> The "Dense Theory" of knowledge extraction is the idea that the LLM should not only extract chunks but also immediately synthesize them into higher-level summaries, creating a "Knowledge Tree" with multiple levels of abstraction. 

* The temp_group: Acts as a "waiting room." Once it hits 5 chunks, it empties itself into the Phase II Aggregator.
* Memory Continuity: When the summary_node is created, it's saved to context_buffer["latest_summary"]. This means chunk #6 will actually "know" the summary of chunks #1–5, helping it stay consistent with the themes already established.
* The "Children" Key: In the final JSON, each summary block now lists which leaf chunks belong to it; making it a Navigable Tree


** The Logic of the Knowledge-Pyramid: **  

* L0 (Leaves): 1-2 pages of raw text rewritten
* L1 (Clusters/Branches): Summary of 5 Leaves (~10 pages)
* L2 (Chapters): Summary of 5 L1 Clusters/Branches (~50 pages)
* L3 (Volume): Summary of all L2 Nodes (The entire book)

** Phase I - Intelligent Chunking/Extract and rewrite chunks (The "Leaves") **

> The Semantic Split: Instead of splitting - at fixed token boundaries, a cursor advances through the text and the LLM finds semantically complete split points, such as - at exactly 1000 characters, we give the LLM a 6000-character window and ask it to find the natural "Topic End" - locations where a topic is probable to end with another to probably begin - via "break_text".

* Self-Sufficiency: Each confirmed chunk gets rewritten into a self-sufficient context, and the prompt tells the LLM to resolve pronouns, making implied subjects explicit, and preserving all specific facts, numbers, and names; in a text where "it" could refer to a concept mentioned three paragraphs ago, this is vital.

* The Cursor: cursor += relative_break_point ensures we never lose our place in a document spanned across thousands of words, hundreds of pages.

* Descriptive: Not only is the original text maintained alongside the rewrite, each chunk also gets a descriptive filename like "psychological types", instead of "chunk-047"

** Phase II - Bottom-up/Incremental Aggregation into Summaries (The "Branches") **

* Summary Block: With about five chunks, system builds a Summary Block

* Continuity: This Summary Block is then fed back into the context_buffer so the next set of Phase I chunks knows what the previous summary was. 

> "Knowledge Tree" is thus created of summaries as branches connecting chunks as leaves