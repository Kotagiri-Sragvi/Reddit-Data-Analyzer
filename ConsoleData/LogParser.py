import json
import os


def parse_subreddit_log(input_txt_path, output_json_path, subreddit_identity):
    """
    Reads a raw terminal text log file, filters out terminal commands/status lines,
    extracts POST_ROW and COMMENT_ROW components, and saves a clean structural JSON file.
    """
    print(f"🔄 Ingesting stream log: '{input_txt_path}'...")

    # Check if the text file exists in the active workspace directory
    if not os.path.exists(input_txt_path):
        print(f"❌ Execution Error: Could not locate '{input_txt_path}' in the current folder.")
        print("   Make sure the text file is placed in the same directory as this script.\n")
        return

    # Base dictionary mapping structure matching the cloud ingest schema
    master_dataset = {
        "subreddit": subreddit_identity,
        "posts": [],
        "comments": []
    }

    # Open with utf-8 encoding to preserve specialized user text, symbols, and emojis safely
    with open(input_txt_path, 'r', encoding='utf-8') as infile:
        for line_num, line in enumerate(infile, start=1):
            line = line.strip()

            # Isolate and unpack post rows
            if line.startswith("POST_ROW:"):
                # Strip only the prefix keyword out from the line
                raw_json_str = line.replace("POST_ROW:", "", 1).strip()
                try:
                    post_object = json.loads(raw_json_str)
                    master_dataset["posts"].append(post_object)
                except json.JSONDecodeError:
                    print(f"⚠️ Warning: Structural anomalies detected on line {line_num} (Post Object skipped).")

            # Isolate and unpack comment rows
            elif line.startswith("COMMENT_ROW:"):
                # Strip only the prefix keyword out from the line
                raw_json_str = line.replace("COMMENT_ROW:", "", 1).strip()
                try:
                    comment_object = json.loads(raw_json_str)
                    master_dataset["comments"].append(comment_object)
                except json.JSONDecodeError:
                    print(f"⚠️ Warning: Structural anomalies detected on line {line_num} (Comment Object skipped).")

    # Serialize and export the clean unified dictionary to disk
    with open(output_json_path, 'w', encoding='utf-8') as outfile:
        json.dump(master_dataset, outfile, indent=4, ensure_ascii=False)

    print(f"✅ Extraction complete! File created: '{output_json_path}'")
    print(f"   📊 Stored Posts:    {len(master_dataset['posts'])}")
    print(f"   📊 Stored Comments: {len(master_dataset['comments'])}\n")


if __name__ == "__main__":
    # Mapping definitions connecting your text log filenames to the target clean json payloads
    data_pipeline_tasks = [
        {
            "txt_file": "Technology-subreddit-data.txt",
            "json_file": "technology_data.json",
            "sub_name": "technology"
        },
        {
            "txt_file": "Gaming-subreddit-data.txt",
            "json_file": "gaming_data.json",
            "sub_name": "gaming"
        },
        {
            "txt_file": "Smartphones-subreddit-data.txt",
            "json_file": "smartphones_data.json",
            "sub_name": "smartphones"
        }
    ]

    print("==================== STARTING LOCAL DATA EXTRACTION PIPELINE ====================\n")

    for task in data_pipeline_tasks:
        parse_subreddit_log(task["txt_file"], task["json_file"], task["sub_name"])

    print("==================== ALL PIPELINE PARSING TASKS COMPLETE ====================")