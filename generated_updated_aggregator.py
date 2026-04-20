
import os

def generate_updated_aggregator():
    script_content = r'''import os
import json
import datetime
import asyncio
import tiktoken
import pymupdf4llm
from groq import Groq
from dotenv import load_dotenv
from pathlib import Path

# 1. SETUP
load_dotenv()
client = Groq(api_key=os.getenv("GROQ_API_KEY"))
MODEL = "llama-3.1-8b-instant"
encoding = tiktoken.get_encoding("cl100k_base")

def call_groq_json(system_prompt, user_content):
    strict_system_prompt = system_prompt + "\nIMPORTANT: Ensure all internal quotes are escaped. Respond ONLY in valid JSON."
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

def generate_summary_block(chunks):
    combined = "\n\n".join([f"{c['filename']}: {c['content']}" for c in chunks])
    prompt = "Synthesize these Jungian chunks into a dense Level-1 summary. JSON keys: 'summary_name', 'synthesis'."
    return call_groq_json(prompt, combined)

async def run_chunking_process(pdf_path, queue=None, whole=False, start_p=20, end_p=30):
    # Setup Directory for Markdown Files
    timestamp = datetime.datetime.now().strftime("%m%d%Y_%H%M")
    md_folder = Path(f"jungian_agent_data_{timestamp}")
    md_folder.mkdir(exist_ok=True)

    # 1. Determine Page Range
    pages_to_read = None if whole else list(range(start_p, end_p))
    print(f"🚀 {'WHOLE BOOK' if whole else f'Pages {start_p}-{end_p}'} processing started...")

    # 2. Extract Markdown
    md_text = pymupdf4llm.to_markdown(str(pdf_path), pages=pages_to_read)
    
    cursor = 0
    all_leaves = []
    summary_blocks = []
    temp_group = []
    CHUNK_GROUP_SIZE = 5
    
    # context_buffer holds the 'rolling state'
    context_buffer = {"predecessor": "Start", "latest_summary": "None"}

    while cursor < len(md_text):
        lookahead = md_text[cursor : cursor + 6000]
        if not lookahead.strip(): break

        prompt = f"Context: {context_buffer['latest_summary']} | Prev: {context_buffer['predecessor'][:200]}...\nExtract a self-sufficient Jungian chunk. JSON keys: 'break_text', 'rewritten_text', 'filename'."
        
        try:
            result = call_groq_json(prompt, lookahead)
            
            # Semantic Jump Logic
            break_text = result.get('break_text', "")
            relative_break = lookahead.find(break_text) + len(break_text) if (break_text and break_text in lookahead) else 2000
            
            new_chunk = {
                "type": "leaf",
                "filename": result.get('filename', 'untitled_chunk').replace(" ", "_"),
                "content": result.get('rewritten_text', ''),
                "parent_summary": context_buffer["latest_summary"]
            }
            
            all_leaves.append(new_chunk)
            temp_group.append(new_chunk)

            # PUSH TO UI
            if queue:
                await queue.put(new_chunk)

            context_buffer["predecessor"] = new_chunk["content"]
            cursor += relative_break

            # PHASE II: AGGREGATION
            if len(temp_group) >= CHUNK_GROUP_SIZE:
                summary_res = generate_summary_block(temp_group)
                
                summary_node = {
                    "type": "summary",
                    "name": summary_res['summary_name'].replace(" ", "_"),
                    "content": summary_res['synthesis'],
                    "children": [c['filename'] for c in temp_group]
                }
                summary_blocks.append(summary_node)
                context_buffer["latest_summary"] = summary_node["content"]
                
                # Update all chunks in this group with their official parent summary
                for c in temp_group:
                    c["parent_summary"] = summary_node["content"]
                    
                    # SAVE CONTEXTUAL MARKDOWN FILE
                    md_filename = md_folder / f"{c['filename']}.md"
                    with open(md_filename, "w", encoding="utf-8") as md_file:
                        md_file.write(f"--- CONTEXT ---\n{summary_node['content']}\n\n")
                        md_file.write(f"--- CONTENT ---\n{c['content']}")

                if queue:
                    await queue.put(summary_node)
                
                temp_group = []

        except Exception as e:
            print(f"Error: {e}")
            cursor += 3000
            continue

    # Final Save of the Master JSON
    final_data = {"leaves": all_leaves, "summaries": summary_blocks}
    with open(f"knowledge_tree_{timestamp}.json", "w") as f:
        json.dump(final_data, f, indent=4)
    
    if queue:
        await queue.put("DONE")
'''
    return script_content

print(generate_updated_aggregator())
