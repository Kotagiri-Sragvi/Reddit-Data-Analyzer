import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter

# ==========================================
# 🛠️ GLOBAL CONFIGURATION & PATH SETUP
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else './'
INPUT_DIR = os.path.join(BASE_DIR, 'LLManalysis')
OUTPUT_DIR = os.path.join(BASE_DIR, 'LLMvisuals')
os.makedirs(OUTPUT_DIR, exist_ok=True)

SUBREDDITS = ['Gaming', 'Smartphones', 'Technology']
LABELS = ['Posts', 'Comments']

# Standardize visual theme for publication-quality reports
sns.set_theme(style="whitegrid")
plt.rcParams.update({
    'font.size': 11,
    'axes.labelsize': 12,
    'axes.titlesize': 14,
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'figure.titlesize': 16
})


# ==========================================
# 📊 STAGE 1: MODEL AGREEMENT AUDIT ENGINE
# ==========================================
def run_model_agreement_audit():
    print("==================================================================", flush=True)
    print("🚀 STAGE 1: INITIALIZING CORE MODEL AGREEMENT AUDIT ENGINE", flush=True)
    print("==================================================================", flush=True)

    audit_lines = [
        "==================================================================",
        "               LLM MODEL AGREEMENT AUDIT REPORT                   ",
        "==================================================================\n"
    ]

    for sub in SUBREDDITS:
        for label in LABELS:
            gemini_sent_file = os.path.join(INPUT_DIR, f"{sub}_{label}_Gemini_Sentiment.csv")
            groq_sent_file = os.path.join(INPUT_DIR, f"{sub}_{label}_Groq_Sentiment.csv")
            gemini_tox_file = os.path.join(INPUT_DIR, f"{sub}_{label}_Gemini_Toxicity.csv")
            groq_tox_file = os.path.join(INPUT_DIR, f"{sub}_{label}_Groq_Toxicity.csv")

            if not (os.path.exists(gemini_sent_file) and os.path.exists(groq_sent_file)):
                continue

            try:
                df_g_sent = pd.read_csv(gemini_sent_file)
                df_q_sent = pd.read_csv(groq_sent_file)
                df_g_tox = pd.read_csv(gemini_tox_file)
                df_q_tox = pd.read_csv(groq_tox_file)

                id_col = [c for c in df_g_sent.columns if 'ID' in c or 'id' in c][0]

                merged_sent = pd.merge(df_g_sent, df_q_sent, on=id_col, suffixes=('_gemini', '_groq'))
                merged_tox = pd.merge(df_g_tox, df_q_tox, on=id_col, suffixes=('_gemini', '_groq'))

                valid_sent = merged_sent[
                    ~merged_sent['sentiment_gemini'].astype(str).str.lower().isin(['skipped', 'unknown'])]
                valid_tox = merged_tox[
                    ~merged_tox['toxicity_gemini'].astype(str).str.lower().isin(['skipped', 'unknown'])]

                if not valid_sent.empty:
                    sent_match = (valid_sent['sentiment_gemini'].str.strip().str.lower() ==
                                  valid_sent['sentiment_groq'].str.strip().str.lower()).mean() * 100
                    sent_report = f"📊 {sub} {label} -> Sentiment Agreement Rate: {sent_match:.2f}% (Sample Window: {len(valid_sent)} rows)"
                else:
                    sent_report = f"📊 {sub} {label} -> Sentiment Agreement Rate: Insufficient valid data pairs."

                if not valid_tox.empty:
                    def clean_tox(val):
                        v = str(val).strip().lower()
                        if v in ['low', 'non-toxic', 'nontoxic', 'safe']: return 'safe'
                        if v in ['medium', 'high', 'toxic']: return 'toxic'
                        return v

                    tox_match = (valid_tox['toxicity_gemini'].apply(clean_tox) ==
                                 valid_tox['toxicity_groq'].apply(clean_tox)).mean() * 100
                    tox_report = f"💀 {sub} {label} -> Toxicity Agreement Rate: {tox_match:.2f}% (Sample Window: {len(valid_tox)} rows)"
                else:
                    tox_report = f"💀 {sub} {label} -> Toxicity Agreement Rate: Insufficient valid data pairs."

                print(sent_report, flush=True)
                print(tox_report, flush=True)
                audit_lines.extend([sent_report, tox_report, ""])

            except Exception as e:
                print(f"⚠️ Error compiling audit layer metrics for {sub} {label}: {e}", flush=True)

    report_path = os.path.join(OUTPUT_DIR, "model_agreement_report.txt")
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(audit_lines))
    print(f"\n💾 Model validation audit trail successfully compiled inside: {report_path}\n", flush=True)


# ==========================================
# 📊 STAGES 2 & 3: VISUALIZATION COMPILATION ENGINE
# ==========================================
def compile_master_datasets():
    master_sentiment = []
    master_toxicity = []
    all_topics_dict = {sub: [] for sub in SUBREDDITS}

    for sub in SUBREDDITS:
        for label in LABELS:
            sent_file = os.path.join(INPUT_DIR, f"{sub}_{label}_Groq_Sentiment.csv")
            tox_file = os.path.join(INPUT_DIR, f"{sub}_{label}_Groq_Toxicity.csv")
            top_file = os.path.join(INPUT_DIR, f"{sub}_{label}_Groq_Topics.csv")

            if os.path.exists(sent_file):
                df = pd.read_csv(sent_file)
                df.columns = [c.lower() for c in df.columns]
                if 'sentiment' in df.columns:
                    for val in df['sentiment'].dropna().astype(str).str.strip().str.capitalize():
                        if val in ['Positive', 'Negative', 'Neutral']:
                            master_sentiment.append({'Subreddit': sub, 'Type': label, 'Sentiment': val})

            if os.path.exists(tox_file):
                df = pd.read_csv(tox_file)
                df.columns = [c.lower() for c in df.columns]
                if 'toxicity' in df.columns:
                    for val in df['toxicity'].dropna().astype(str).str.strip().str.lower():
                        status = 'Toxic' if val in ['high', 'medium', 'toxic'] else 'Non-Toxic'
                        master_toxicity.append({'Subreddit': sub, 'Type': label, 'Status': status})

            if os.path.exists(top_file):
                df = pd.read_csv(top_file)
                df.columns = [c.lower() for c in df.columns]
                if 'topics' in df.columns:
                    for raw_topics in df['topics'].dropna().astype(str):
                        # Clean and split topics dynamically
                        tokens = [t.strip().title() for t in
                                  raw_topics.replace('[', '').replace(']', '').replace('"', '').replace("'", "").split(
                                      ',') if t.strip()]
                        all_topics_dict[sub].extend(tokens)

    return pd.DataFrame(master_sentiment), pd.DataFrame(master_toxicity), all_topics_dict


def generate_visualizations():
    print("==================================================================", flush=True)
    print("🚀 STAGES 2 & 3: ASSEMBLING NUMERICAL METRICS CHARTS", flush=True)
    print("==================================================================", flush=True)

    df_sent, df_tox, topics_map = compile_master_datasets()

    # ----------------------------------------------------------------
    # CHART 1: SENTIMENT DISTRIBUTIONS (POSTS VS COMMENTS PANEL)
    # ----------------------------------------------------------------
    if not df_sent.empty:
        print("📈 Constructing Sentiment Distribution Bar Plots...", flush=True)
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))

        for i, label in enumerate(LABELS):
            sub_df = df_sent[df_sent['Type'] == label]
            if sub_df.empty: continue

            counts = sub_df.groupby(['Subreddit', 'Sentiment']).size().unstack(fill_value=0)
            pcts = counts.div(counts.sum(axis=1), axis=0) * 100
            pcts_df = pcts.reset_index().melt(id_vars='Subreddit', value_name='Percentage')

            sns.barplot(
                data=pcts_df, x='Subreddit', y='Percentage', hue='Sentiment',
                palette={'Positive': '#2ecc71', 'Neutral': '#95a5a6', 'Negative': '#e74c3c'},
                ax=axes[i]
            )
            axes[i].set_title(f"Linguistic Sentiment Breakdowns: {label}")
            axes[i].set_ylabel("Percentage of Total Dataset (%)")
            axes[i].set_xlabel("Subreddit Forum Category")
            axes[i].set_ylim(0, 100)

        plt.suptitle("Linguistic Sentiment Profile Distributions Across Communities", y=0.98)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, "sentiment_distributions.png"), dpi=300)
        plt.close()

    # ----------------------------------------------------------------
    # CHART 2: SUBREDDIT COMPARISON (TOXICITY PREVALENCE)
    # ----------------------------------------------------------------
    if not df_tox.empty:
        print("📊 Building Subreddit Comparative Toxicity Dashboards...", flush=True)
        plt.figure(figsize=(10, 6))

        tox_counts = df_tox.groupby(['Subreddit', 'Status']).size().unstack(fill_value=0)
        tox_pcts = tox_counts.div(tox_counts.sum(axis=1), axis=0) * 100
        tox_pcts_df = tox_pcts.reset_index()

        if 'Toxic' not in tox_pcts_df.columns:
            tox_pcts_df['Toxic'] = 0.0

        tox_pcts_df = tox_pcts_df.sort_values(by='Toxic', ascending=False)

        sns.barplot(
            data=tox_pcts_df, x='Subreddit', y='Toxic',
            palette="Oranges_r", edgecolor="#d35400", linewidth=1.5
        )

        plt.title("Comparative Toxic Density Ratios Across Monitored Communities")
        plt.ylabel("Prevalence Frequency of Toxic Anomalies (%)")
        plt.xlabel("Subreddit Forum Category")
        plt.ylim(0, max(tox_pcts_df['Toxic'].max() + 5, 15))

        for idx, row in enumerate(tox_pcts_df.itertuples()):
            plt.text(idx, row.Toxic + 0.4, f"{row.Toxic:.1f}%", ha='center', fontweight='bold', color='#d35400')

        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, "subreddit_toxicity_comparison.png"), dpi=300)
        plt.close()

    # ----------------------------------------------------------------
    # CHART 3: FIXED QUANTITATIVE TOPIC FREQUENCIES (HORIZONTAL BAR CHARTS)
    # ----------------------------------------------------------------
    print("📊 Generating Numerical Topic Frequency Bar Charts...", flush=True)
    for sub in SUBREDDITS:
        tokens = topics_map[sub]
        if not tokens:
            continue

        # Filter noise/meta words
        filtered_tokens = [t for t in tokens if t.lower() not in [
            'phone', 'comment', 'post', 'tech', 'gaming', 'smartphone', 'general', 'unknown', 'threads'
        ]]

        if not filtered_tokens:
            continue

        # Get numerical counts for Top 10 topics
        frequency_counts = Counter(filtered_tokens).most_common(10)
        df_top_topics = pd.DataFrame(frequency_counts, columns=['Topic Tag', 'Occurrence Count'])

        plt.figure(figsize=(10, 5.5))

        # Generate horizontal numerical barplot
        ax = sns.barplot(
            data=df_top_topics,
            x='Occurrence Count',
            y='Topic Tag',
            palette="Blues_r",
            edgecolor="#2980b9",
            linewidth=1.2
        )

        # Draw exact numerical counts right on the edge of the bars
        for p in ax.patches:
            width = p.get_width()
            ax.text(
                width + (max(df_top_topics['Occurrence Count']) * 0.01),
                p.get_y() + p.get_height() / 2 + 0.1,
                f'{int(width)}',
                ha="left", va="center", fontweight='bold', color='#2c3e50'
            )

        plt.title(f"Top 10 Extracted Metric Topic Spaces: r/{sub}", pad=15)
        plt.xlabel("Absolute Numerical Occurrence Count")
        plt.ylabel("AI-Classified Topic Labels")
        plt.xlim(0, max(df_top_topics['Occurrence Count']) * 1.12)  # Generous buffer for counts text

        plt.tight_layout()
        filename = f"topic_frequency_{sub.lower()}_bar_chart.png"
        plt.savefig(os.path.join(OUTPUT_DIR, filename), dpi=300)
        plt.close()

    print("\n==================================================================", flush=True)
    print("✨ SUCCESS! ALL CRITICAL NUMERICAL CHARTS GENERATED WITH EXIT CODE 0!", flush=True)
    print(f"Open your workspace directory folder: '{OUTPUT_DIR}/'", flush=True)
    print("==================================================================", flush=True)


if __name__ == "__main__":
    run_model_agreement_audit()
    generate_visualizations()
