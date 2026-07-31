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
    raise ValueError("CRITICAL: Missing API keys in your .env file. Ensure GEMINI_API_KEY and GROQ_API_KEY are set.")

# Model Engine Configurations
GEMINI_MODEL_NAME = "gemini-3.5-flash"
GROQ_MODEL_NAME = "llama-3.1-8b-instant"

# Configure Google Gemini
genai.configure(api_key=GEMINI_API_KEY)

# Configure Groq
groq_client = Groq(api_key=GROQ_API_KEY)

# Track Engine Quota State
_last_gemini_call_time = 0.0
_gemini_exhausted = False

# Ensure Output Directory Exists
OUTPUT_DIR = "LLManalysis"
os.makedirs(OUTPUT_DIR, exist_ok=True)
AUDIT_LOG_PATH = os.path.join(OUTPUT_DIR, "llm_execution_audit.csv")


# ---------------------------------------------------------------------------
# 2. AUDIT TRAIL LOGGING SYSTEM
# ---------------------------------------------------------------------------
def append_to_audit_log(identifiers, prompt, model, output, errors):
    """
    Maintains a persistent transaction sheet documenting every single API call event.
    """
    log_entry = {
        "input_text_identifiers": str(identifiers),
        "prompt_used": prompt,
        "model_api_used": model,
        "generated_output": output,
        "errors_encountered": errors
    }
    df = pd.DataFrame([log_entry])
    if not os.path.exists(AUDIT_LOG_PATH):
        df.to_csv(AUDIT_LOG_PATH, index=False)
    else:
        df.to_csv(AUDIT_LOG_PATH, mode='a', header=False, index=False)


# ---------------------------------------------------------------------------
# 3. RESILIENT API CALL WRAPPERS
# ---------------------------------------------------------------------------
def call_gemini(prompt, identifiers):
    """
    Calls Gemini 3.5 Flash while enforcing a strict 13.0s pacing cooldown and
    handling reactive exceptions for daily quota thresholds.
    """
    global _last_gemini_call_time, _gemini_exhausted
    if _gemini_exhausted:
        print("[GEMINI] Skipped call. Daily quota (RPD) flag is active.", flush=True)
        return None

    # Enforce strict pacing buffer to safely stay under the 5 RPM constraint
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
            return raw_text

        except Exception as e:
            err_msg = str(e)
            print(f"[GEMINI WARNING] Exception caught: {err_msg}", flush=True)
            tb_str = traceback.format_exc()
            append_to_audit_log(identifiers, prompt, GEMINI_MODEL_NAME, "", tb_str)

            # Detect explicit Daily Quota Limits
            if "quota" in err_msg.lower() or "exhausted" in err_msg.lower() or "429" in err_msg.lower():
                if "daily" in err_msg.lower() or retries == 1:
                    print("[GEMINI CRITICAL] Daily quota (RPD) wall encountered. Suspending Gemini pipeline tier.",
                          flush=True)
                    _gemini_exhausted = True
                    return None

            print("[GEMINI] Encountered spike/drop. Retrying in 25 seconds...", flush=True)
            time.sleep(25.0)
            retries -= 1

    print("[GEMINI ERROR] All retries exhausted for this request slice.", flush=True)
    return None


def call_groq(prompt, identifiers):
    """
    Calls Groq (Llama 3.1 8b instant) using forced JSON mode objects and
    pacing thresholds between consecutive chunk sweeps.
    """
    # Standard pacing buffer to control TPM/RPM spikes
    time.sleep(3.5)

    retries = 3
    while retries > 0:
        try:
            print(f"[GROQ] Transmitting request for batch IDs: {identifiers}...", flush=True)
            response = groq_client.chat.completions.create(
                model=GROQ_MODEL_NAME,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            raw_text = response.choices[0].message.content

            append_to_audit_log(identifiers, prompt, GROQ_MODEL_NAME, raw_text, "None")
            return raw_text

        except Exception as e:
            err_msg = str(e)
            print(f"[GROQ WARNING] Exception caught: {err_msg}", flush=True)
            tb_str = traceback.format_exc()
            append_to_audit_log(identifiers, prompt, GROQ_MODEL_NAME, "", tb_str)

            print("[GROQ] Rate variance or drop encountered. Retrying in 25 seconds...", flush=True)
            time.sleep(25.0)
            retries -= 1

    print("[GROQ ERROR] All retries exhausted for this sweep chunk.", flush=True)
    return None


# ---------------------------------------------------------------------------
# 4. ROBUST DATA PARSING & KEY SELF-HEALING DEFENSES
# ---------------------------------------------------------------------------
def normalize_dict_keys(d):
    """ Recursively transforms all dictionary keys to lowercase to prevent key schema mismatches. """
    if not isinstance(d, dict):
        return d
    return {str(k).lower(): normalize_dict_keys(v) if isinstance(v, (dict, list)) else v for k, v in d.items()}


def parse_row_results(raw_json_str):
    """
    Validates flat arrays vs structured tracking object formats,
    safely normalizing fields for error-free downstream lookups.
    """
    if not raw_json_str:
        return []
    try:
        data = json.loads(raw_json_str)
        if isinstance(data, dict):
            data = normalize_dict_keys(data)
            # Self-healing: Search fields to find the target data array
            for k, v in data.items():
                if isinstance(v, list):
                    return v
            return [data]
        elif isinstance(data, list):
            return [normalize_dict_keys(item) if isinstance(item, dict) else item for item in data]
    except Exception as e:
        print(f"[PARSE ERROR] Failed to clean JSON payload: {e}", flush=True)
    return []


def align_results(parsed_items, batch_ids):
    """
    Pairs output arrays with true baseline IDs. Implements a positional fallback
    self-healing check if tracking object keys are stripped or renamed by the model.
    """
    aligned = {str(b_id): ("Unknown", "Unknown", "Unknown") for b_id in batch_ids}
    if not parsed_items:
        return aligned

    matched_indices = set()

    # Strategy A: Attempt logical key matching
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

    # Strategy B: Positional Fallback if payload array matches batch count
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
    """ Parses corporate string content payload structures cleanly. """
    default_res = ("No summary generated.", "No patterns identified.")
    if not raw_json_str:
        return default_res
    try:
        data = json.loads(raw_json_str)
        if isinstance(data, dict):
            data = normalize_dict_keys(data)
            summary = data.get('summary', 'No summary generated.')
            patterns_val = data.get('patterns', 'No patterns identified.')
            if isinstance(patterns_val, list):
                patterns_str = " | ".join([str(p) for p in patterns_val])
            else:
                patterns_str = str(patterns_val)
            return str(summary), patterns_str
    except Exception as e:
        print(f"[PARSE ERROR] Failed to process macro schema: {e}", flush=True)
    return default_res


# ---------------------------------------------------------------------------
# 5. LINGUISTIC SCHEMA INJECTIONS (PROMPTS)
# ---------------------------------------------------------------------------
def make_row_prompt(rows_data):
    prompt = (
        "Perform a rigorous linguistic analysis on the following items. "
        "For each text row, accurately identify and return:\n"
        "1. sentiment: (Positive, Negative, Neutral)\n"
        "2. toxicity: (Low, Medium, High)\n"
        "3. topics: A short comma-separated list of primary core keywords.\n\n"
        "Return the response strictly inside a JSON object wrapper with a 'results' list array. "
        "Ensure every JSON object item preserves the mapping identifier string field exactly as provided.\n"
        "Example format:\n"
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
        "1. A structural 3-sentence summary highlighting core subject discussions, overall consensus tone, and user behavior.\n"
        "2. Exactly 3 distinct prominent recurring linguistic conversation patterns observed in the content.\n\n"
        "Return the response strictly as a structured JSON object containing keys 'summary' (string) "
        "and 'patterns' (a list array of exactly 3 strings).\n"
        "Example format:\n"
        "{\n"
        "  \"summary\": \"The text focuses primarily on... Users display a mixed response to... The primary consensus is...\",\n"
        "  \"patterns\": [\n"
        "    \"Frequent deployment of platform jargon.\",\n"
        "    \"Sarcastic complaints regarding pricing strategies.\",\n"
        "    \"Technical feature comparison matrices between generations.\"\n"
        "  ]\n"
        "}\n\n"
        f"Corpus Text Core Sample:\n{concatenated_text}"
    )


def chunk_list(lst, n):
    """ Yields sequential n-sized chunks from a list. """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ---------------------------------------------------------------------------
# 6. PIPELINE WORKFLOW EXECUTION ENGINE
# ---------------------------------------------------------------------------
def process_pipeline():
    subreddits = ['Gaming', 'Smartphones', 'Technology']
    labels = ['posts', 'comments']

    for sub in subreddits:
        for label in labels:
            filename = f"{sub}_{label}.csv"
            if not os.path.exists(filename):
                print(
                    f"[FILE WARNING] Source data file '{filename}' missing in current workspace. Skipping configuration.",
                    flush=True)
                continue

            print(f"\n==================================================================", flush=True)
            print(f"STARTING ASYMMETRIC ANALYTIC RUN FOR SOURCE: {filename.upper()}", flush=True)
            print(f"==================================================================", flush=True)

            # Load raw configurations
            df = pd.read_csv(filename)
            if df.empty:
                print(f"[DATA WARNING] Dataframe '{filename}' is empty. Skipping analysis matrix.", flush=True)
                continue

            # Layout Schema Transformations
            if label == 'comments':
                id_col = 'comment ID'
                df['processed_text'] = df['comment text'].fillna('').astype(str)
            else:
                id_col = 'post ID'
                df['processed_text'] = (
                            df['title'].fillna('').astype(str) + ' ' + df['body/self-text'].fillna('').astype(str))

            # Sanitize tracking IDs to uniform strings
            df[id_col] = df[id_col].astype(str).str.strip()

            # Create standard mapping payloads
            all_rows_payload = [{'id': row[id_col], 'text': row['processed_text']} for _, row in df.iterrows()]

            # Compile Corpus text sample layer (First 20-25 records consolidated)
            corpus_sample_list = df['processed_text'].head(25).tolist()
            compiled_corpus_str = "\n".join(
                [f"Content block [{i}]: {text}" for i, text in enumerate(corpus_sample_list)])
            macro_prompt = make_macro_prompt(compiled_corpus_str)

            # ----------------------------------------------------------------
            # LAYER A: MACRO ANALYSIS LAYER (Gemini 1 Call, Groq 1 Call)
            # ----------------------------------------------------------------
            print(f"[{filename}] Triggering Macro Analysis Summary Blocks...", flush=True)

            gemini_summary, gemini_patterns = "Skipped", "Skipped"
            if not _gemini_exhausted:
                g_macro_raw = call_gemini(macro_prompt, "MACRO_CORPUS_SLICE")
                if g_macro_raw:
                    gemini_summary, gemini_patterns = parse_macro_results(g_macro_raw)

            q_macro_raw = call_groq(macro_prompt, "MACRO_CORPUS_SLICE")
            groq_summary, groq_patterns = parse_macro_results(q_macro_raw)

            # ----------------------------------------------------------------
            # LAYER B: GOOGLE GEMINI TARGETED ROW-LEVEL SLICE (Exactly 1 Batch Request)
            # ----------------------------------------------------------------
            print(f"[{filename}] Deploying Gemini Row Slice Matrix (Rows 0-44)...", flush=True)
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

            # ----------------------------------------------------------------
            # LAYER C: GROQ FULL DATASET DEEP SWEEP (100% Sweep, Batch Size = 5)
            # ----------------------------------------------------------------
            print(f"[{filename}] Initializing Groq Deep Sweep Engine (100% coverage)...", flush=True)
            groq_row_aligned = {}

            for chunk in chunk_list(all_rows_payload, 5):
                chunk_ids = [item['id'] for item in chunk]
                q_row_prompt = make_row_prompt(chunk)
                q_row_raw = call_groq(q_row_prompt, chunk_ids)

                parsed_q_items = parse_row_results(q_row_raw)
                chunk_aligned = align_results(parsed_q_items, chunk_ids)
                groq_row_aligned.update(chunk_aligned)

            # ----------------------------------------------------------------
            # LAYER D: COMPILING UNCORRUPTED OUTPUT FILES
            # ----------------------------------------------------------------
            print(f"[{filename}] Recompiling metrics maps into individual sheets...", flush=True)

            # Data Containers
            g_sentiment_rows, g_toxicity_rows, g_topics_rows = [], [], []
            q_sentiment_rows, q_toxicity_rows, q_topics_rows = [], [], []

            for idx, row in df.iterrows():
                r_id = row[id_col]

                # Gemini Rows Resolution Processing
                if idx < 45 and r_id in gemini_row_aligned:
                    g_sent, g_tox, g_top = gemini_row_aligned[r_id]
                else:
                    g_sent, g_tox, g_top = "Skipped", "Skipped", "Skipped"

                g_sentiment_rows.append({id_col: r_id, "sentiment": g_sent})
                g_toxicity_rows.append({id_col: r_id, "toxicity": g_tox})
                g_topics_rows.append({id_col: r_id, "topics": g_top})

                # Groq Rows Resolution Processing
                if r_id in groq_row_aligned:
                    q_sent, q_tox, q_top = groq_row_aligned[r_id]
                else:
                    q_sent, q_tox, q_top = "Unknown", "Unknown", "Unknown"

                q_sentiment_rows.append({id_col: r_id, "sentiment": q_sent})
                q_toxicity_rows.append({id_col: r_id, "toxicity": q_tox})
                q_topics_rows.append({id_col: r_id, "topics": q_top})

            # Build Output File Mapping Paths
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

            # Write 10 Individual Partition Sheets Explicitly to Folder
            for out_name, out_df in outputs.items():
                target_path = os.path.join(OUTPUT_DIR, out_name)
                out_df.to_csv(target_path, index=False)

            print(f"[SUCCESS] Exported all 10 analysis sheets for matrix segment: {filename}.", flush=True)


# ---------------------------------------------------------------------------
# 7. MAIN RUN SYSTEM EXECUTION GATE
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("Initializing Asymmetric Linguistic Analysis System Pipeline...", flush=True)
    start_time = time.time()

    try:
        process_pipeline()
        print("\n==================================================================", flush=True)
        print("PIPELINE PROCESSING TASK COMPLETE WITHOUT ERRORS.", flush=True)
        print(f"Total processing runtime: {round((time.time() - start_time) / 60, 2)} minutes.", flush=True)
        print(f"All final exports are located within folder: '{OUTPUT_DIR}/'", flush=True)
        print("==================================================================", flush=True)
    except Exception as critical_err:
        print(f"\n[CRITICAL FAILURE] Pipeline processing crashed: {critical_err}", flush=True)
        traceback.print_exc()
