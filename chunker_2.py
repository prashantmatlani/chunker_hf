
# ./phase0102_chunker_aggregator_2.py

"""

https://www.linkedin.com/pulse/new-way-encode-documents-ai-agents-navigable-trees-sergii-makarevych-a6cof/

https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f


 ----- The Logic of the Knowledge-Pyramid: ----- 

L0 (Leaves): 1-2 pages of raw text rewritten
L1 (Clusters/Branches): Summary of 5 Leaves (~10 pages)
L2 (Chapters): Summary of 5 L1 Clusters/Branches (~50 pages)
L3 (Volume): Summary of all L2 Nodes (The entire book)

The combined script - with two phases, I and II, fired sequentially - aligns with a/ the "Dense Theory" of knowledge extraction and b/ with Makarevych's "Incremental Aggregation" logic of the availabity of a set of chunks triggering the system's to generate a summary. The "Dense Theory" of knowledge extraction is the idea that the LLM should not only extract chunks but also immediately synthesize them into higher-level summaries, creating a "Knowledge Tree" with multiple levels of abstraction. 

. The temp_group: Acts as a "waiting room." Once it hits 5 chunks, it empties itself into the Phase II Aggregator.
. Memory Continuity: When the summary_node is created, it's saved to context_buffer["latest_summary"]. This means chunk #6 will actually "know" the summary of chunks #1–5, helping it stay consistent with the themes already established.
. The "Children" Key: In the final JSON, each summary block now lists which leaf chunks belong to it. This is what makes it a Navigable Tree.


> Phase I - Extract and rewrite chunks (The "Leaves")

The Semantic Split: Instead of splitting at exactly 1000 characters, we give the LLM a 6000-character window and ask it to find the natural "Topic End" (break_text).

Self-Sufficiency: The prompt tells the LLM to resolve pronouns; in a text where "it" could refer to a concept mentioned three paragraphs ago, this is vital.

The Cursor: cursor += relative_break_point ensures we never lose our place in a document spanned across thousands of words, hundreds of pages.


> Phase II - Incremental Aggregation into Summaries (The "Branches")

Summary Block: With about five chunks, system builds a Summary Block

Continuity: This Summary Block is then fed back into the context_buffer so the next set of Phase I chunks knows what the previous summary was. 

"Knowledge Tree" is thus created of summaries as branches connecting chunks as leaves

"""

import os
import json
import datetime
import asyncio
import tiktoken
import pymupdf4llm
from groq import Groq

from dotenv import load_dotenv
from pathlib import Path

import time 
import datetime
import sys


load_dotenv()
client = Groq(api_key=os.getenv("GROQ_API_KEY"))
MODEL = "llama-3.1-8b-instant"
encoding = tiktoken.get_encoding("cl100k_base")

# 2. Define the folder and the filename
#pdf_folder = Path("C:\\Users\\wd052\\OneDrive\\Desktop\\00\\01\\PDFs\\J\\CW") 
#pdf_path = r"C:\Users\wd052\OneDrive\Desktop\00\01\PDFs\J\CW\Collected Works of Dr. C.G. Jung - Vol. 6 - Psychological-Types.pdf"
#pdf_folder = Path("C:/Users/wd052/OneDrive/Desktop/00/01/PDFs/J/CW") 
#pdf_name = "Collected Works of Dr. C.G. Jung - Vol. 6 - Psychological-Types.pdf"

# Combine them
#pdf_path = pdf_folder / pdf_name

#WHOLE = False # Set to True to process the whole book; False to process a page range
#START_PAGE = 8
#END_PAGE = 10

laf = 2000 # look-ahead factor
djf = 0.1 # dynamic jump factor

async def call_groq_json(system_prompt, user_content):
    strict_system_prompt = system_prompt + "\nIMPORTANT: Ensure all internal quotes are escaped. Respond ONLY in valid JSON."

    # Use loop.run_in_executor to keep the Groq call from blocking the UI
    loop = asyncio.get_event_loop()
    completion = await loop.run_in_executor(
        None, 
        lambda: client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": strict_system_prompt},
                {"role": "user", "content": user_content}
            ],
            response_format={"type": "json_object"},
            temperature=0.2 # Lower temperature = more stable JSON; the LLM is less "creative" with formatting at temperature of 0.2, and more likely to follow a perfect JSON structure
        )
    )
    
    # LLM can technically generate multiple different versions of an answer if its asked to
    # Groq returns these as a list called "choices", since even a single answer is inside a list,  Python must be told to look at index 0 to get the actual content 
    # Then we access the "message" key, followed by "content" key to get the raw JSON string
    return json.loads(completion.choices[0].message.content)

# - 1 to START PAGE; Python's range(5, 7) gives pages 6 and 7, to get to the exact specified range we do START_PAGE-1
# Alignment: Convert Human (1-indexed) to Library (0-indexed)
# Human page 5 is internal page 4
#async def run_chunking_process(pdf_path, queue=None, whole=WHOLE, start_p=START_PAGE-1, end_p=END_PAGE):
async def run_chunking_process(pdf_path, queue=None, whole=False, start_p=1, end_p=1):
    """
    Main entry point for the chunking logic.
    If queue is provided, it 'yields' results to the UI.
    """
    #print(f"\nwhole: {whole}, start_p: {start_p}, end_p: {end_p}")

    # 1. Determine Page Range
    if whole:
        # PyMuPDF4LLM uses None to process all pages
        pages_to_read = None 
        print("📚 Processing the WHOLE book...")
    else:
        # start_p-1 -> adjustment for 0-indexing
        pages_to_read = list(range(int(start_p-1), int(end_p)))
        #print(f"📑 Processing pages {START_PAGE} to {END_PAGE}...") # for print purposes subtract and add back 1 from start and end pages, aligning with those specified in the code

    # 2. Extract Markdown
    md_text = pymupdf4llm.to_markdown(str(pdf_path), pages=pages_to_read)
    
    # Returns a list of dictionaries, one for each page
    #pagesscanned = pymupdf4llm.to_markdown("your_document.pdf", page_chunks=True)
    allpages = pymupdf4llm.to_markdown(str(pdf_path), page_chunks=True)

    pages_data = pymupdf4llm.to_markdown(str(pdf_path), pages=pages_to_read, page_chunks=True)
    
    print(f"📖 Page-Aware Engine Started. Total Pages to process: {len(pages_data)}")

    # pull page number from the chunk's metadata
    for page in pages_data:
        # Extract metadata from this specific page
        current_page_text = page["text"]
        real_page_num = page["metadata"].get("page_number", "??")

    """
    # Instead of a single string of text, we have a list to pull directly the page numbers being scanned from each chunk's metadata
    for p in pagesscanned:
        real_page_num = p["metadata"]["page_number"] # This is the real-time detected page
        text_content = p["text"]
    """
    # --- Initialize the number of characters permitted to be skipped, depending on the total number of words in the document ---
    total_len = len(md_text)
    
    # DYNAMIC JUMP: 10% of text or 2000 chars
    #dynamic_jump = min(2000, max(500, int(total_len * 0.1)))
    dynamic_jump = min(2000, max(500, int(total_len * djf)))
    # --- Initialize the number of characters permitted to be skipped, depending on the total number of words in the document - End ---
    
    print(f"filepath -> {pdf_path}")
    print(f"\n# of words -> {total_len}; dynamic jump at -> {dynamic_jump}")

    cursor = 0
    l0_buffer = [] # Holds Leaves for L1 (Clusters/Branches)
    #l1_buffer = [] # Holds L1 Summaries for L2 (Chapters)
    #l2_buffer = [] # Holds L2 Summaries for L3 (Volumes)

    all_leaves = []     # Final collection
    all_l1_summaries = []
    all_l2_summaries = []
    l3_node = None      # The final crown

    l_buffer_size = 5 # CHUNK_GROUP_SIZE

    #all_leaves = []
    #summary_blocks = []
    #temp_group = []
    #CHUNK_GROUP_SIZE = 5
    
    context_buffer = {"predecessor": "Start", "latest_summary": "None"}

    while cursor < len(md_text):
        #lookahead = md_text[cursor : cursor + 6000]
        lookahead = md_text[cursor : cursor + laf]

        # ---- DEBUG: Print first 50 characters to see the starting sentence ----
        start_snippet = lookahead[:80].replace('\n', ' ')
        print(f"🔍 DEBUG: Cursor at {cursor}. Current text starts with: '{start_snippet}'")
        
        # Since pymupdf4llm inserts page markers like '----- Page 5 -----', we search backwards from the cursor to find the last page tag/number
        current_page_search = md_text[:cursor].rfind("Page ")
        if current_page_search != -1:
            page_num = md_text[current_page_search:current_page_search+10]
            print(f"📖 DEBUG: Currently scanning near {page_num}")
        # ---- DEBUG: Print first 50 characters to see the starting sentence - End ----

        if not lookahead.strip(): break

        #prompt = f"Context: {context_buffer['latest_summary']} | Prev: {context_buffer['predecessor'][:200]}...\nExtract a self-sufficient Jungian chunk. JSON keys: 'break_text', 'rewritten_text', 'filename'."
        
        try:
            # --- PHASE I: CREATE L0 LEAF ---
            prompt = "Extract self-sufficient Jungian chunk. JSON: 'break_text', 'rewritten_text', 'filename'."

            # Note: Ensure call_groq_json is an async function or run in executor
            res = await call_groq_json(prompt, lookahead)
            
            leaf = {"type": "leaf", "page": real_page_num, "name": res['filename'], "content": res['rewritten_text']}

            all_leaves.append(leaf)
            l0_buffer.append(leaf) # stack-up leaves

            #  PUSH TO UI
            if queue: await queue.put(leaf)

            # --- PHASE II: AGGREGATE LEAVES; TRIGGER L1 (Every 5 Leaves) ---
            if len(l0_buffer) >= l_buffer_size:
                print("⭐ Creating L1 Cluster...")
                l1_res = await generate_summary_block(l0_buffer, "Level-1 Cluster")
                l1_node = {"type": "summary_l1", "name": l1_res['summary_name'], "content": l1_res['synthesis']}
                
                all_l1_summaries.append(l1_node)
                #l1_buffer.append(l1_node) # stack-up clusters/branches
                
                if queue: await queue.put(l1_node)
                
                l0_buffer = [] # Reset L0

            # --- PHASE III: TRIGGER L2 (Every 5 L1 Clusters) ---
            #if len(l1_buffer) >= l_buffer_size:
            if len(all_l1_summaries) >= l_buffer_size and len(all_l1_summaries) % 5 == 0:
                print("💎 Creating L2 Chapter...")
                # We take the last 5 L1s

                l2_res = await generate_summary_block(all_l1_summaries[-5:], "Level-2 Chapter")

                l2_node = {"type": "summary_l2", "name": l2_res['summary_name'], "content": l2_res['synthesis']}
                
                all_l2_summaries.append(l2_node)
                #l2_buffer.append(l2_node) # stack-up chapters
                if queue: await queue.put(l2_node)
                l1_buffer = [] # Reset L1

            # Process the break and update cursor; also "result.get(...)" prevents crashes if keys are missing
            # Semantic Jump Logic, find the break text and move cursor
            break_text = res.get('break_text', "")
            cursor += (lookahead.find(break_text) + len(break_text)) if break_text in lookahead else laf # laf -> 2000
            
            # Calculate exactly where the chunk ends
            if break_text in lookahead:
                end_index = lookahead.find(break_text) + len(break_text)
            else:
                end_index = laf # Fallback

            # This captures ONLY the text analyzed for this specific leaf
            actual_original_text = lookahead[:end_index]

            new_chunk = {
            "type": "leaf",
            "filename": res.get('filename', 'untitled'),
            "content": res.get('rewritten_text', ''),
            "page_num": page["metadata"]["page_number"], # capture page number
            "original": actual_original_text, # Save a snippet of the original
            }

            # Throttling to stay under 6000 TPM limit
            await asyncio.sleep(7)

        except Exception as e:
            if "429" in str(e):
                print("  ⚠️ Rate limited! Cooling down for 30 seconds...")
                time.sleep(30)
            print(f"❌ ERROR AT CURSOR {cursor}: {e}") 
            #print(f"Error: {e}")
            #cursor += 2000
            cursor += dynamic_jump # Use our automated jump
            await asyncio.sleep(10) # Longer pause on error
            continue

    # --- FINAL FLUSH (The "Cleanup" Phase) ---
    # If the book ends and we have leftover leaves (1-4), summarize them now!
    if l0_buffer:
        l1_res = await generate_summary_block(l0_buffer, "Final Level-1 Cluster")
        l1_node = {"type": "summary_l1", "name": l1_res['summary_name'], "content": l1_res['synthesis']}
        all_l1_summaries.append(l1_node)
        if queue: await queue.put(l1_node)

    # Summarize all L1s into L2 if we haven't already
    if all_l1_summaries and not all_l2_summaries:
        l2_res = await generate_summary_block(all_l1_summaries, "Level-2 Chapter")
        l2_node = {"type": "summary_l2", "name": l2_res['summary_name'], "content": l2_res['synthesis']}
        all_l2_summaries.append(l2_node)
        if queue: await queue.put(l2_node)

    # FINAL VOLUME SUMMARY (L3)
    if all_l2_summaries:
        l3_res = await generate_summary_block(all_l2_summaries, "Level-3 Volume")
        l3_node = {"type": "summary_l3", "name": l3_res['summary_name'], "content": l3_res['synthesis']}
        if queue: await queue.put(l3_node)

    #if queue: await queue.put("DONE")


    # --- THE SAFE SAVE ---
    timestamp = datetime.datetime.now().strftime("%m%d%Y_%H%M")
    #final_data = {
    #    "metadata": {"pages": f"{start_p}-{end_p}", "date": timestamp},
    #    "leaves": all_leaves,
    #    "l1_clusters": all_l1_summaries,
    #    "l2_chapters": all_l2_summaries,
    #    "l3_volume": l3_node
    #}
    #"""
    final_data = {
                #"metadata": {"pages": f"{allpages}", "date": timestamp},
                #"metadata": {"page_number": f"{page_num}", "date": timestamp},
                "metadata": {"pages": f"{start_p}-{end_p}", "date": timestamp},
                "date": timestamp,
                "leaves": all_leaves,
                "l1_clusters": all_l1_summaries,
                "l2_chapters": all_l2_summaries,
                "l3_volume": l3_node}
    #"""
    output_file = f"knowledge_tree_{timestamp}.json"
    with open(output_file, "w") as f:
        json.dump(final_data, f, indent=4)

    # CALL TO CREATE NESTED AND TABULAR MARKDOWNs
    export_visual_formats(final_data, timestamp) 

    if queue: await queue.put("DONE")

"""
# Helper for summary
async def generate_summary_block(chunks):
    combined = "\n\n".join([f"{c['filename']}: {c['content']}" for c in chunks])
    prompt = "Synthesize these Jungian chunks into a single high-density Level-1 summary. JSON keys: 'summary_name', 'synthesis'."
    
    return await call_groq_json(prompt, combined)
"""

# Add 'label' as a second parameter with a default value
async def generate_summary_block(chunks_to_summarize, label="Level-1 Cluster"):
    combined_content = "\n\n".join([f"Source: {c['name']}\n{c['content']}" for c in chunks_to_summarize])
    
    # We use the 'label' in the prompt to help the LLM understand the scale
    system_prompt = f"""
    You are creating a '{label}' for a Knowledge Tree of Carl Jung's work.
    
    TASK:
    Synthesize the provided content into a single, high-density summary.
    - DO NOT say 'This section covers...'.
    - DO say 'Psychological concepts in this section include...'
    - Maintain the information density of the original inputs.
    
    RESPONSE FORMAT (JSON):
    {{
      "summary_name": "thematic_cluster_name",
      "synthesis": "the dense summary text"
    }}
    """
    return await call_groq_json(system_prompt, combined_content)

"""
Nested Markdown 

Contextual Integrity - Acts as a "Read Me" for the Jungian Agent. It can follow the # headers to understand the hierarchy.
Auditability: By including the SOURCE TEXT vs AI INTERPRETATION, it becomes possible to verify whether the LLM is "hallucinating" terms like individuation or if it's a valid AI interpretation in the Jungian sense, owing to the alchemical symbols.

Table Markdown 

Visual Clarity: Table Markdown is perfect for a quick bird's-eye view, such as the number of chunks under each chapter 
"""
# --- NESTED AND TABULAR MARKDOWN
def export_visual_formats(final_data, timestamp):
    # --- NESTED MARKDOWN ---

    # --- Uncoment the below to include the whole text - 'pages' - of the document in generated "nested_knowledge_xxxx" markdown and in json, useful in the case of short documents, articles, papers, etc. ---
    #md_nested = f"# 👑 VOLUME: {final_data['metadata']['pages']}\n" 
    #md_nested = f"# 👑 VOLUME: {final_data['metadata']['page_num']}\n" 
    md_nested = f"# 👑 VOLUME SUMMARY\n"
    md_nested += f"> {final_data['l3_volume']['content'] if final_data['l3_volume'] else 'N/A'}\n\n"
    
    for l2 in final_data['l2_chapters']:
        md_nested += f"## 💎 CHAPTER: {l2['name']}\n> {l2['content']}\n\n"
        # Logic to associate children would go here; for now, we list all relevant nodes
        for l1 in final_data['l1_clusters']:
            md_nested += f"### ⭐ CLUSTER: {l1['name']}\n> {l1['content']}\n\n"
            for leaf in final_data['leaves']:
                page_label = f" (Page {leaf.get('page_num', '??')})"
                md_nested += f"#### 📄 [LEAF]: {leaf['name']}\n"
                md_nested += f"**[AI INTERPRETATION]:** {leaf['content']}\n\n"
                md_nested += f"**[ORIGINAL TEXT]:** {leaf.get('original', 'N/A')[:250]}...\n\n---\n"

    # --- TABULAR MARKDOWN ---
    md_table = "| Volume (L3) | Chapter (L2) | Cluster/Summary (L1) | Page | Chunk (L0) |\n"
    md_table += "| :--- | :--- | :--- | :--- | :--- |\n"
    
    l3_name = final_data['l3_volume']['name'] if final_data['l3_volume'] else "Volume"
    
    for l2 in final_data['l2_chapters']:
        l2_name = l2['name']
        l2_summary = l2['content'][:100] + "..."
        
        for l1 in final_data['l1_clusters']:
            l1_name = l1['name']
            l1_summary = l1['content'][:100] + "..."
            
            for leaf in final_data['leaves']:
                leaf_name = leaf['name']
                # Include page number in the table for extra clarity
                pg = leaf.get('page_num', '??')
                leaf_content = f"**[{pg} AI]** " + leaf['content'][:150] + "..."
                orig_text = leaf.get('original', 'N/A')[:100] + "..."

                md_table += f"| 👑 VOLUME: {l3_name} | 💎 CHAPTER: **{l2_name}**: {l2_summary} | **⭐ CLUSTER: {l1_name}**: {l1_summary} | {pg} | 📄 LEAF: {leaf_content} | ORIGINAL: {orig_text} | \n"


    # Save files
    with open(f"nested_knowledge_{timestamp}.md", "w", encoding="utf-8") as f: f.write(md_nested)
    with open(f"table_knowledge_{timestamp}.md", "w", encoding="utf-8") as f: f.write(md_table)

    
    print(f"✅ Created: \n\nVisual Markdowns: \nnested_knowledge_{timestamp}.md  \ntable_knowledge_{timestamp}.md \n\nand JSON: \n\nknowledge_tree_{timestamp}.json")