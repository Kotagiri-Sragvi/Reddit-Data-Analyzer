import os
import time
import json
import traceback
import pandas as pd
from dotenv import load_dotenv

# Import AI Engine SDKs
import google.generativeai as genai
from groq import Groq

# ---------------------------------------------------------------------------
# 1. SETUP & INITIALIZATION
# ---------------------------------------------------------------------------
load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

if not GEMINI_API_KEY or not GROQ_API_KEY:
    raise ValueError("CRITICAL: Missing API keys in your .env file.")

GEMINI_MODEL_NAME = "gemini-3.5-flash"
GROQ_MODEL_NAME = "llama-3.1-8b-instant"

genai.configure(api_key=GEMINI_API_KEY)
groq_client = Groq(api_key=GROQ_API_KEY)

# State tracking flags
_last_gemini_call_time = 0.0
_gemini_exhausted = False  # Set to False so we can utilize 2 of your 7 remaining requests!

OUTPUT_DIR = "LLManalysis"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Points directly to your existing audit log to preserve your history
AUDIT_LOG_PATH = os.path.join(OUTPUT_DIR, "llm_execution_audit.csv")


# ---------------------------------------------------------------------------
# 2. AUDIT TRAIL LOGGING SYSTEM (SAFE APPEND MODE)
# ---------------------------------------------------------------------------
def append_to_audit_log(identifiers, prompt, model, output, errors):
    """
    Safely documents every transaction. If the file already exists from previous
    runs, it uses mode='a' and disables headers to append without overwriting.
    """
    log_entry = {
        "input_text_identifiers": str(identifiers),
        "prompt_used": prompt.strip().replace("\n", " "),
        "model_api_used": model,
        "generated_output": str(output).strip().replace("\n", " "),
        "errors_encountered": str(errors)
    }
    df = pd.DataFrame([log_entry])

    if not os.path.exists(AUDIT_LOG_PATH):
        df.to_csv(AUDIT_LOG_PATH, index=False)
    else:
        # Crucial: Appends cleanly to preserve Gaming, Smartphones, and Tech Posts data
        df.to_csv(AUDIT_LOG_PATH, mode='a', header=False, index=False)


# ---------------------------------------------------------------------------
# 3. FIXED & PACED API WRAPPERS
# ---------------------------------------------------------------------------
def call_gemini(prompt, identifiers):
    """ Calls Gemini while strictly maintaining the 5 RPM constraint. """
    global _last_gemini_call_time, _gemini_exhausted
    if _gemini_exhausted:
        print("[GEMINI] Skipped call. Safe limit gate is active.", flush=True)
        return None

    # Strict pacing buffer to stay under 5 RPM
    elapsed = time.time() - _last_gemini_call_time
    if elapsed < 13.0:
        time.sleep(13.0 - elapsed)

    retries = 3
    while retries > 0:
        try:
            print(f"[GEMINI] Transmitting request for IDs: {identifiers}...", flush=True)
            _last_gemini_call_time = time.time()

            model = genai.GenerativeModel(GEMINI_MODEL_NAME)
            response = model.generate_content(
                prompt,
                generation_config={"response_mime_type": "application/json"}
            )
            raw_text = response.text
            append_to_audit_log(identifiers, prompt, GEMINI_MODEL_NAME, raw_text, "None")

            # Enforce immediate cooldown right after success
            time.sleep(13.0)
            return raw_text
        except Exception as e:
            err_msg = str(e).lower()
            print(f"[GEMINI WARNING] Exception caught: {err_msg}", flush=True)
            append_to_audit_log(identifiers, prompt, GEMINI_MODEL_NAME, "", str(e))

            if "day" in err_msg or "daily" in err_msg or "quota" in err_msg:
                print("[GEMINI CRITICAL] Daily quota boundary detected. Disengaging Gemini tier.", flush=True)
                _gemini_exhausted = True
                return None

            print("[GEMINI] Congestion detected. Retrying in 30 seconds...", flush=True)
            time.sleep(30.0)
            retries -= 1
    return None


def call_groq(prompt, identifiers):
    """ Calls Groq with a single, clear pacing delay to protect TPM windows. """
    # Single unified pacing delay to prevent token spikes
    time.sleep(6.5)

    retries = 4
    while retries > 0:
        try:
            print(f"[GROQ] Transmitting chunk batch IDs: {identifiers}...", flush=True)
            response = groq_client.chat.completions.create(
                model=GROQ_MODEL_NAME,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            raw_text = response.choices[0].message.content
            append_to_audit_log(identifiers, prompt, GROQ_MODEL_NAME, raw_text, "None")
            return raw_text
        except Exception as e:
            print(f"[GROQ WARNING] Rate limit variance caught. Cooling down...", flush=True)
            append_to_audit_log(identifiers, prompt, GROQ_MODEL_NAME, "", str(e))
            print("[GROQ] Enforcing token bucket flush window. Retrying in 40 seconds...", flush=True)
            time.sleep(40.0)
            retries -= 1
    return None


# ---------------------------------------------------------------------------
# 4. ROBUST PARSING ENGINE MODULES
# ---------------------------------------------------------------------------
def normalize_dict_keys(d):
    if not isinstance(d, dict):
        return d
    return {str(k).lower(): normalize_dict_keys(v) if isinstance(v, (dict, list)) else v for k, v in d.items()}


def parse_row_results(raw_json_str):
    if not raw_json_str:
        return []
    try:
        data = json.loads(raw_json_str)
        if isinstance(data, dict):
            data = normalize_dict_keys(data)
            for k, v in data.items():
                if isinstance(v, list):
                    return v
            return [data]
        elif isinstance(data, list):
            return [normalize_dict_keys(item) if isinstance(item, dict) else item for item in data]
    except Exception as e:
        print(f"[PARSE ERROR] Failed to clean JSON layer: {e}", flush=True)
    return []


def align_results(parsed_items, batch_ids):
    aligned = {str(b_id): ("Unknown", "Unknown", "Unknown") for b_id in batch_ids}
    if not parsed_items:
        return aligned

    matched_indices = set()
    for item in parsed_items:
        if not isinstance(item, dict):
            continue
        item_id = None
        for k, v in item.items():
            if 'id' in k:
                item_id = str(v).strip()
                break

        if item_id and item_id in aligned:
            sentiment = item.get('sentiment', 'Unknown')
            toxicity = item.get('toxicity', 'Unknown')
            topics = item.get('topics', 'Unknown')
            aligned[item_id] = (str(sentiment), str(toxicity), str(topics))
            try:
                matched_indices.add(batch_ids.index(item_id))
            except ValueError:
                pass

    if len(parsed_items) == len(batch_ids):
        for i, b_id in enumerate(batch_ids):
            if aligned[b_id] == ("Unknown", "Unknown", "Unknown") or i not in matched_indices:
                item = parsed_items[i]
                if isinstance(item, dict):
                    sentiment = item.get('sentiment', 'Unknown')
                    toxicity = item.get('toxicity', 'Unknown')
                    topics = item.get('topics', 'Unknown')
                    aligned[b_id] = (str(sentiment), str(toxicity), str(topics))
    return aligned


def parse_macro_results(raw_json_str):
    default_res = ("No summary generated.", "No patterns identified.")
    if not raw_json_str:
        return default_res
    try:
        data = json.loads(raw_json_str)
        if isinstance(data, dict):
            data = normalize_dict_keys(data)
            summary = data.get('summary', 'No summary generated.')
            patterns_val = data.get('patterns', 'No patterns identified.')
            patterns_str = " | ".join([str(p) for p in patterns_val]) if isinstance(patterns_val, list) else str(
                patterns_val)
            return str(summary), patterns_str
    except Exception as e:
        print(f"[PARSE ERROR] Failed to extract macro structures: {e}", flush=True)
    return default_res


def make_row_prompt(rows_data):
    prompt = (
        "Perform a rigorous linguistic analysis on the following items. "
        "For each text row, accurately identify and return:\n"
        "1. sentiment: (Positive, Negative, Neutral)\n"
        "2. toxicity: (Low, Medium, High)\n"
        "3. topics: A short comma-separated list of primary core keywords.\n\n"
        "Return the response strictly inside a JSON object wrapper with a 'results' list array. "
        "Preserve the mapping identifier string field exactly.\n"
        "{\n"
        "  \"results\": [\n"
        "    {\"id\": \"example_id\", \"sentiment\": \"Neutral\", \"toxicity\": \"Low\", \"topics\": \"tech, hardware\"}\n"
        "  ]\n"
        "}\n\n"
        "Target payload inputs to scan:\n"
    )
    for r in rows_data:
        prompt += f"ID: {r['id']} | Text: {r['text']}\n"
    return prompt


def make_macro_prompt(concatenated_text):
    return (
        "Analyze the following social media text compilation layer corpus sample. "
        "Provide a high-level metadata abstraction containing:\n"
        "1. A structural 3-sentence summary highlighting core subject discussions, overall tone, and user behavior.\n"
        "2. Exactly 3 distinct prominent recurring linguistic conversation patterns observed in the content.\n\n"
        "Return the response strictly as a structured JSON object containing keys 'summary' and 'patterns' (list of 3 strings).\n"
        f"Corpus Text Core Sample:\n{concatenated_text}"
    )


def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ---------------------------------------------------------------------------
# 5. CORE DEDICATED EXECUTION ISOLATION LAYER
# ---------------------------------------------------------------------------
def run_isolated_target():
    filename = "Technology_comments.csv"
    sub, label = "Technology", "comments"

    if not os.path.exists(filename):
        print(f"❌ Critical Error: Source data file '{filename}' missing inside local directory.")
        return

    print(f"\n==================================================================", flush=True)
    print(f"🚀 INITIALIZING RUN FOR DEPLOYMENT SEGMENT: {filename.upper()}", flush=True)
    print(f"==================================================================", flush=True)

    df = pd.read_csv(filename)
    if df.empty:
        print("❌ Error: Target file dataframe is empty.")
        return

    id_col = 'comment ID'
    df['processed_text'] = df['comment text'].fillna('').astype(str)
    df[id_col] = df[id_col].astype(str).str.strip()

    all_rows_payload = [{'id': row[id_col], 'text': row['processed_text']} for _, row in df.iterrows()]

    # Corpus Synthesis Layer
    corpus_sample_list = df['processed_text'].head(25).tolist()
    compiled_corpus_str = "\n".join([f"Content block [{i}]: {text}" for i, text in enumerate(corpus_sample_list)])
    macro_prompt = make_macro_prompt(compiled_corpus_str)

    # --- STEP 1: MACRO ANALYSIS LAYER (Uses exactly 1 request per model) ---
    print(f"📈 Generating Macro Summary Metadata Aggregations...", flush=True)
    gemini_summary, gemini_patterns = "Skipped", "Skipped"

    if not _gemini_exhausted:
        g_macro_raw = call_gemini(macro_prompt, "MACRO_CORPUS_SLICE")
        if g_macro_raw:
            gemini_summary, gemini_patterns = parse_macro_results(g_macro_raw)

    q_macro_raw = call_groq(macro_prompt, "MACRO_CORPUS_SLICE")
    groq_summary, groq_patterns = parse_macro_results(q_macro_raw)

    # --- STEP 2: GEMINI TARGETED ROW SLICE (Uses exactly 1 request total) ---
    print(f"🟢 Deploying Gemini targeted comparative slice (Rows 0-44)...", flush=True)
    gemini_row_aligned = {}

    if not _gemini_exhausted:
        gemini_slice_payload = all_rows_payload[:45]
        gemini_slice_ids = [item['id'] for item in gemini_slice_payload]
        if gemini_slice_payload:
            g_row_prompt = make_row_prompt(gemini_slice_payload)
            g_row_raw = call_gemini(g_row_prompt, gemini_slice_ids)
            if g_row_raw:
                parsed_g_items = parse_row_results(g_row_raw)
                gemini_row_aligned = align_results(parsed_g_items, gemini_slice_ids)

    # --- STEP 3: GROQ FULL DEEP SWEEP (Batch size = 5, single pacing delay) ---
    print(f"🔵 Ingesting Groq deep analysis sweep (Batch size = 5 for balanced speed)...", flush=True)
    groq_row_aligned = {}

    for chunk in chunk_list(all_rows_payload, 5):
        chunk_ids = [item['id'] for item in chunk]
        q_row_prompt = make_row_prompt(chunk)
        q_row_raw = call_groq(q_row_prompt, chunk_ids)

        parsed_q_items = parse_row_results(q_row_raw)
        chunk_aligned = align_results(parsed_q_items, chunk_ids)
        groq_row_aligned.update(chunk_aligned)

    # --- STEP 4: GENERATE OUTPUT CSV FILES ---
    print(f"💾 Compiling metric sets into final decoupled tracking spreads...", flush=True)
    g_sentiment_rows, g_toxicity_rows, g_topics_rows = [], [], []
    q_sentiment_rows, q_toxicity_rows, q_topics_rows = [], [], []

    for idx, row in df.iterrows():
        r_id = row[id_col]

        # Gemini Row Mapping Resolvers
        if idx < 45 and r_id in gemini_row_aligned:
            g_sent, g_tox, g_top = gemini_row_aligned[r_id]
        else:
            g_sent, g_tox, g_top = "Skipped", "Skipped", "Skipped"

        g_sentiment_rows.append({id_col: r_id, "sentiment": g_sent})
        g_toxicity_rows.append({id_col: r_id, "toxicity": g_tox})
        g_topics_rows.append({id_col: r_id, "topics": g_top})

        # Groq Row Mapping Resolvers
        if r_id in groq_row_aligned:
            q_sent, q_tox, q_top = groq_row_aligned[r_id]
        else:
            q_sent, q_tox, q_top = "Unknown", "Unknown", "Unknown"

        q_sentiment_rows.append({id_col: r_id, "sentiment": q_sent})
        q_toxicity_rows.append({id_col: r_id, "toxicity": q_tox})
        q_topics_rows.append({id_col: r_id, "topics": q_top})

    outputs = {
        f"{sub}_{label}_Gemini_Sentiment.csv": pd.DataFrame(g_sentiment_rows),
        f"{sub}_{label}_Gemini_Toxicity.csv": pd.DataFrame(g_toxicity_rows),
        f"{sub}_{label}_Gemini_Topics.csv": pd.DataFrame(g_topics_rows),
        f"{sub}_{label}_Gemini_Summarization.csv": pd.DataFrame([{"summary": gemini_summary}]),
        f"{sub}_{label}_Gemini_Patterns.csv": pd.DataFrame([{"patterns": gemini_patterns}]),

        f"{sub}_{label}_Groq_Sentiment.csv": pd.DataFrame(q_sentiment_rows),
        f"{sub}_{label}_Groq_Toxicity.csv": pd.DataFrame(q_toxicity_rows),
        f"{sub}_{label}_Groq_Topics.csv": pd.DataFrame(q_topics_rows),
        f"{sub}_{label}_Groq_Summarization.csv": pd.DataFrame([{"summary": groq_summary}]),
        f"{sub}_{label}_Groq_Patterns.csv": pd.DataFrame([{"patterns": groq_patterns}]),
    }

    for out_name, out_df in outputs.items():
        target_path = os.path.join(OUTPUT_DIR, out_name)
        out_df.to_csv(target_path, index=False)

    print(f"\n==================================================================", flush=True)
    print(f"✨ SUCCESS! All 10 final analysis spreadsheets for {filename} exported flawlessly.", flush=True)
    print("All transaction logs have been securely appended to your central audit sheet.", flush=True)
    print(f"==================================================================", flush=True)


if __name__ == "__main__":
    run_isolated_target()
