
# ./phase0102_chunker_aggregator_2.py

"""

https://www.linkedin.com/pulse/new-way-encode-documents-ai-agents-navigable-trees-sergii-makarevych-a6cof/

https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f


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

WHOLE = False # Set to True to process the whole book; False to process a page range
START_PAGE = 8
END_PAGE = 10

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
            temperature=0.2
        )
    )
    return json.loads(completion.choices[0].message.content)

    """
    completion = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": strict_system_prompt},
            {"role": "user", "content": user_content}
        ],
        response_format={"type": "json_object"},
        temperature=0.2
    )
    return json.loads(completion.choices[0].message.content)
    """

#async def run_chunking_process(pdf_path, queue=None, whole=False, start_p=20, end_p=30):
# + 1 to END PAGE; Python's range(5, 7) gives pages 5 and 6, to include page 7, we need range(5, 8)
async def run_chunking_process(pdf_path, queue=None, whole=WHOLE, start_p=START_PAGE, end_p=END_PAGE+1):
    """
    Main entry point for the chunking logic.
    If queue is provided, it 'yields' results to the UI.
    """
    print(f"\nwhole: {whole}, start_p: {start_p}, end_p: {end_p}")

    # 1. Determine Page Range
    if whole:
        # PyMuPDF4LLM uses None to process all pages
        pages_to_read = None 
        print("📚 Processing the WHOLE book...")
    else:
        pages_to_read = list(range(start_p, end_p))
        print(f"📑 Processing pages {start_p} to {end_p}...")

    # 2. Extract Markdown
    md_text = pymupdf4llm.to_markdown(str(pdf_path), pages=pages_to_read)
    
    # --- Initialize the number of characters permitted to be skipped, depending on the total number of words in the document ---
    total_len = len(md_text)
    
    # DYNAMIC JUMP: 10% of text or 2000 chars
    dynamic_jump = min(2000, max(500, int(total_len * 0.1)))
    # --- Initialize the number of characters permitted to be skipped, depending on the total number of words in the document - End ---
    
    print(f"filepath -> {pdf_path}")
    print(f"\n# of words -> {total_len}; dynamic jump at -> {dynamic_jump}")

    cursor = 0
    all_leaves = []
    summary_blocks = []
    temp_group = []
    CHUNK_GROUP_SIZE = 5
    
    context_buffer = {"predecessor": "Start", "latest_summary": "None"}

    while cursor < len(md_text):
        lookahead = md_text[cursor : cursor + 6000]

        # ---- DEBUG: Print first 50 characters to see the starting sentence ----
        start_snippet = lookahead[:50].replace('\n', ' ')
        print(f"🔍 DEBUG: Cursor at {cursor}. Current text starts with: '{start_snippet}'")
        
        # Since pymupdf4llm inserts page markers like '----- Page 5 -----', we search backwards from the cursor to find the last page tag/number
        current_page_search = md_text[:cursor].rfind("Page ")
        if current_page_search != -1:
            page_num = md_text[current_page_search:current_page_search+10]
            print(f"📖 DEBUG: Currently scanning near {page_num}")
        # ---- DEBUG: Print first 50 characters to see the starting sentence - End ----

        if not lookahead.strip(): break

        prompt = f"Context: {context_buffer['latest_summary']} | Prev: {context_buffer['predecessor'][:200]}...\nExtract a self-sufficient Jungian chunk. JSON keys: 'break_text', 'rewritten_text', 'filename'."
        
        try:
            # Note: Ensure call_groq_json is an async function or run in executor
            result = await call_groq_json(prompt, lookahead)
            
            # Semantic Jump Logic
            break_text = result.get('break_text', "")
            relative_break = lookahead.find(break_text) + len(break_text) if break_text in lookahead else 2000
            
            new_chunk = {
                "type": "leaf",
                "filename": result.get('filename', 'untitled'),
                "content": result.get('rewritten_text', '')
            }
            
            all_leaves.append(new_chunk)
            temp_group.append(new_chunk)

            # PUSH TO UI
            if queue:
                await queue.put(new_chunk)

            context_buffer["predecessor"] = new_chunk["content"]
            # Throttling to stay under 6000 TPM limit
            await asyncio.sleep(7) 
            cursor += relative_break

            # PHASE II: AGGREGATION
            if len(temp_group) >= CHUNK_GROUP_SIZE:
                from phase0102_chunker_aggregator_2 import generate_summary_block # Ensure helper is available
                summary_res = await generate_summary_block(temp_group)
                
                summary_node = {
                    "type": "summary",
                    "name": summary_res['summary_name'],
                    "content": summary_res['synthesis'],
                    "children": [c['filename'] for c in temp_group]
                }
                summary_blocks.append(summary_node)
                context_buffer["latest_summary"] = summary_node["content"]
                
                if queue:
                    await queue.put(summary_node)
                
                temp_group = []

            # 5-second pause after every chunk to stay under TPM limits
            print("  ⏳ Throttling for 5s to avoid Rate Limits...")
            time.sleep(5)

        except Exception as e:
            if "429" in str(e):
                print("  ⚠️ Rate limited! Cooling down for 30 seconds...")
                time.sleep(30)

            print(f"Error: {e}")
            #cursor += 3000
            cursor += dynamic_jump # Use our automated jump
            await asyncio.sleep(10) # Longer pause on error

            continue

    if queue: await queue.put("DONE")

    # Final Save
    timestamp = datetime.datetime.now().strftime("%m%d%Y_%H%M")
    final_data = {"leaves": all_leaves, "summaries": summary_blocks}
    with open(f"knowledge_tree_{timestamp}.json", "w") as f:
        json.dump(final_data, f, indent=4)
    
    if queue:
        await queue.put("DONE")

# Helper for summary
async def generate_summary_block(chunks):
    combined = "\n\n".join([f"{c['filename']}: {c['content']}" for c in chunks])
    prompt = "Synthesize these Jungian chunks into a single high-density Level-1 summary. JSON keys: 'summary_name', 'synthesis'."
    return await call_groq_json(prompt, combined)
