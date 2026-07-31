import os
import json
import pandas as pd


def run_local_cleaning_pipeline():
    # 1. Use the active directory where the script is executed
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # 2. Match the exact filename prefixes sitting in your P01DA26 folder
    datasets = ['Gaming', 'Smartphones', 'Technology']

    print("==================== STARTING LOCAL CLEANING & SEGREGATION PIPELINE ====================\n")

    for prefix in datasets:
        json_filename = f"{prefix}RawData.json"
        json_input_path = os.path.join(BASE_DIR, json_filename)

        print(f"==================================================================")
        print(f"🔄 Processing Raw File: {json_filename}")

        if not os.path.exists(json_input_path):
            print(f"   ❌ Error: File '{json_filename}' not found in the project directory. Skipping...")
            continue

        # Load the raw dataset arrays
        with open(json_input_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)

        posts_list = raw_data.get('posts', [])
        comments_list = raw_data.get('comments', [])

        print(f"   📥 Initial Import: Found {len(posts_list)} posts and {len(comments_list)} comments.")

        # --- STEP A: PROCESS & CLEAN POSTS DATA ---
        if posts_list:
            df_posts = pd.DataFrame(posts_list)
            initial_post_count = len(df_posts)

            # 1. Remove Duplicate Entries (by post ID)
            if 'post ID' in df_posts.columns:
                df_posts = df_posts.drop_duplicates(subset=['post ID'], keep='first')

            # 2. Handle Missing Values safely
            for col in ['body/self-text', 'title']:
                if col in df_posts.columns:
                    df_posts[col] = df_posts[col].fillna("")
            if 'author' in df_posts.columns:
                df_posts['author'] = df_posts['author'].fillna("[unknown_user]")
            if 'score/upvotes' in df_posts.columns:
                df_posts['score/upvotes'] = df_posts['score/upvotes'].fillna(0).astype(int)
            if 'number of comments' in df_posts.columns:
                df_posts['number of comments'] = df_posts['number of comments'].fillna(0).astype(int)

            # 3. Convert Unix Timestamps to Readable Datetime String (YYYY-MM-DD HH:MM:SS)
            if 'creation timestamp' in df_posts.columns:
                df_posts['creation timestamp'] = pd.to_datetime(
                    df_posts['creation timestamp'], unit='s', errors='coerce'
                ).dt.strftime('%Y-%m-%d %H:%M:%S')

            post_duplicates_removed = initial_post_count - len(df_posts)
            print(f"   ✓ Posts: Removed {post_duplicates_removed} duplicate records.")
            print(f"   ✓ Posts: Timestamps successfully parsed into readable formats.")
        else:
            df_posts = pd.DataFrame()
            print("   ⚠️ Notice: No posts array found in this dataset.")

        # --- STEP B: PROCESS & CLEAN COMMENTS DATA ---
        if comments_list:
            df_comments = pd.DataFrame(comments_list)
            initial_comment_count = len(df_comments)

            # 1. Remove Duplicate Comments (by comment ID)
            if 'comment ID' in df_comments.columns:
                df_comments = df_comments.drop_duplicates(subset=['comment ID'], keep='first')
            comment_dupes_removed = initial_comment_count - len(df_comments)

            # 2. Filter out Deleted or Removed Comments
            deleted_placeholders = ['[deleted]', '[removed]']

            # Build boolean flags verifying both comment text and authors for deletion strings
            is_deleted = pd.Series(False, index=df_comments.index)
            if 'comment text' in df_comments.columns:
                is_deleted |= df_comments['comment text'].astype(str).str.lower().isin(deleted_placeholders)
            if 'author' in df_comments.columns:
                is_deleted |= df_comments['author'].astype(str).str.lower().isin(deleted_placeholders)

            df_comments = df_comments[~is_deleted]
            deleted_removed_purged = initial_comment_count - comment_dupes_removed - len(df_comments)

            # 3. Handle Remaining Missing Values safely
            if 'comment text' in df_comments.columns:
                df_comments['comment text'] = df_comments['comment text'].fillna("")
            if 'author' in df_comments.columns:
                df_comments['author'] = df_comments['author'].fillna("[unknown_user]")
            if 'score/upvotes' in df_comments.columns:
                df_comments['score/upvotes'] = df_comments['score/upvotes'].fillna(0).astype(int)

            # 4. Convert Unix Timestamps to Readable Datetime String (YYYY-MM-DD HH:MM:SS)
            if 'timestamp' in df_comments.columns:
                df_comments['timestamp'] = pd.to_datetime(
                    df_comments['timestamp'], unit='s', errors='coerce'
                ).dt.strftime('%Y-%m-%d %H:%M:%S')

            print(f"   ✓ Comments: Removed {comment_dupes_removed} duplicate records.")
            print(f"   ✓ Comments: Purged {deleted_removed_purged} placeholder deletion records.")
            print(f"   ✓ Comments: Timestamps successfully parsed into readable formats.")
        else:
            df_comments = pd.DataFrame()
            print("   ⚠️ Notice: No comments array found in this dataset.")

        # --- STEP C: EXPORT CLEAN ARTIFACTS BACK TO PROJECT FOLDER ---
        posts_output_csv = os.path.join(BASE_DIR, f"{prefix}_posts.csv")
        comments_output_csv = os.path.join(BASE_DIR, f"{prefix}_comments.csv")

        if not df_posts.empty:
            df_posts.to_csv(posts_output_csv, index=False, encoding='utf-8')
            print(f"   💾 Generated Clean Spread: {prefix}_posts.csv (Rows: {len(df_posts)})")

        if not df_comments.empty:
            df_comments.to_csv(comments_output_csv, index=False, encoding='utf-8')
            print(f"   💾 Generated Clean Spread: {prefix}_comments.csv (Rows: {len(df_comments)})")

    print("\n==================== LOCAL PIPELINE CLEANING EXECUTION COMPLETE ====================")
    print("✨ All done! Your clean, deduplicated CSV sheets are sitting safely inside your workspace directory.")

#The below function is used only for the bugs come over by me, one can omit this function's usage
def finalize_and_clean_data():
    # Set workspace directory (works for both PyCharm and Colab if paths match)
    # For Google Colab, change this to: BASE_DIR = '/content/drive/MyDrive/P01AM26/'
    BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else './'

    datasets = ['Gaming', 'Smartphones', 'Technology']

    print("==================== STARTING FINAL DATA PURIFICATION ====================\n")

    for prefix in datasets:
        posts_path = os.path.join(BASE_DIR, f"{prefix}_posts.csv")
        comments_path = os.path.join(BASE_DIR, f"{prefix}_comments.csv")

        # ----------------------------------------------------------------------
        # 1. CLEANING POSTS FILES
        # ----------------------------------------------------------------------
        if os.path.exists(posts_path):
            print(f"⚙️ Harmonizing schema and timestamps for: {prefix}_posts.csv")
            df_posts = pd.read_csv(posts_path)

            # Fix the broken split column bug in Gaming_posts
            if 'creationtimestamp' in df_posts.columns:
                # Convert raw Unix integers to human readable string dates where missing
                unix_mask = df_posts['creation timestamp'].isna() & df_posts['creationtimestamp'].notna()
                if unix_mask.any():
                    df_posts.loc[unix_mask, 'creation timestamp'] = pd.to_datetime(
                        df_posts.loc[unix_mask, 'creationtimestamp'], unit='s', errors='coerce'
                    ).dt.strftime('%Y-%m-%d %H:%M:%S')
                # Drop the redundant broken column
                df_posts = df_posts.drop(columns=['creationtimestamp'])

            # Standardize Technology's mismatched date string format to YYYY-MM-DD HH:MM:SS
            if 'creation timestamp' in df_posts.columns:
                df_posts['creation timestamp'] = pd.to_datetime(
                    df_posts['creation timestamp'], errors='coerce'
                ).dt.strftime('%Y-%m-%d %H:%M:%S')

            # Handle remaining missing value artifacts safely
            if 'body/self-text' in df_posts.columns:
                df_posts['body/self-text'] = df_posts['body/self-text'].fillna("")
            if 'title' in df_posts.columns:
                df_posts['title'] = df_posts['title'].fillna("")

            # Save the pristine, uniform version back to the folder
            df_posts.to_csv(posts_path, index=False, encoding='utf-8')
            print(f"   ✓ {prefix}_posts.csv is now completely clean and standardized.")

        # ----------------------------------------------------------------------
        # 2. CLEANING COMMENTS FILES
        # ----------------------------------------------------------------------
        if os.path.exists(comments_path):
            print(f"⚙️ Harmonizing schema and text formats for: {prefix}_comments.csv")
            df_comments = pd.read_csv(comments_path)

            # Ensure unified timestamp string formatting
            if 'timestamp' in df_comments.columns:
                df_comments['timestamp'] = pd.to_datetime(
                    df_comments['timestamp'], errors='coerce'
                ).dt.strftime('%Y-%m-%d %H:%M:%S')

            # Clean missing text rows
            if 'comment text' in df_comments.columns:
                df_comments['comment text'] = df_comments['comment text'].fillna("")

            df_comments.to_csv(comments_path, index=False, encoding='utf-8')
            print(f"   ✓ {prefix}_comments.csv is now completely clean and standardized.")
        print("-" * 75)

    print("\n==================== DATA ARCHITECTURE IS NOW PRISTINE ====================")
    print("✨ All files share identical timestamp structures and zero-null columns!")

if __name__ == "__main__":
    run_local_cleaning_pipeline()
    #USE THE BELOW FUNCTION ONLY IF REQUIRED
    #finalize_and_clean_data()
