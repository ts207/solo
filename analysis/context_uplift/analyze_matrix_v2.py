import pandas as pd
import numpy as np

# Load data
matrix = pd.read_csv('/home/irene/Edge/analysis/context_uplift/matrix.csv')

# Get baseline metrics per slice for positive_funding
pf_df = matrix[(matrix['context_cell'] == 'positive_funding') & (matrix['survived'] == True)]
pf_slice_max = pf_df.groupby('slice_label')['rank_score'].max().to_dict()

# Compute metrics per context_cell
results = []
contexts = matrix[matrix['context_cell'] != 'unconditional']['context_cell'].unique()

for ctx in contexts:
    df_ctx = matrix[matrix['context_cell'] == ctx]
    survived_df = df_ctx[df_ctx['survived'] == True]
    
    # Slices where this context survived and beat unconditional
    positive_uplift_df = survived_df[survived_df['uplift_bps_vs_unconditional'] > 0]
    
    surviving_slices_count = survived_df['slice_label'].nunique()
    slice_wins_vs_unconditional = positive_uplift_df['slice_label'].nunique()
    
    # Wins vs positive funding
    slice_wins_vs_pf = 0
    for slice_label, group in survived_df.groupby('slice_label'):
        ctx_max_rank = group['rank_score'].max()
        pf_max_rank = pf_slice_max.get(slice_label, -1)
        if ctx_max_rank > pf_max_rank:
            slice_wins_vs_pf += 1

    max_fold_valid_count = survived_df['fold_valid_count'].max() if not survived_df.empty else 0
    max_rank_score = survived_df['rank_score'].max() if not survived_df.empty else 0
    median_uplift = df_ctx['uplift_bps_vs_unconditional'].median()
    
    if not survived_df.empty:
        best_row = survived_df.loc[survived_df['rank_score'].idxmax()]
        best_event_type = best_row['event_type']
        best_symbol = best_row['symbol']
    else:
        best_event_type = "N/A"
        best_symbol = "N/A"

    # User's Explicit Keepers:
    if ctx == 'positive_funding':
        bucket = 'CORE'
    elif ctx in ['chop', 'bullish_trend', 'negative_funding']:
        bucket = 'EXPERIMENTAL'
    elif ctx in ['bearish_trend', 'high_vol', 'wide_spread', 'low_vol']:
        bucket = 'DEMOTE'
    else:
        # Fallback to the strict rule if unknown
        if slice_wins_vs_unconditional >= 2 and max_fold_valid_count >= 2 and slice_wins_vs_pf > 0:
            bucket = 'CORE'
        elif slice_wins_vs_unconditional >= 1:
            bucket = 'EXPERIMENTAL'
        else:
            bucket = 'DEMOTE'

    results.append({
        'Context Cell': ctx,
        'Surviving Slices': surviving_slices_count,
        'Slice Wins vs Uncond': slice_wins_vs_unconditional,
        'Slice Wins vs PF': slice_wins_vs_pf,
        'Best Event Type': best_event_type,
        'Best Symbol': best_symbol,
        'Median Uplift vs Uncond (bps)': round(median_uplift, 2) if pd.notnull(median_uplift) else np.nan,
        'Max Rank Score': round(max_rank_score, 4),
        'Max Fold Valid Count': int(max_fold_valid_count) if pd.notnull(max_fold_valid_count) else 0,
        'Keeper Bucket': bucket
    })

# Convert to DataFrame and sort
res_df = pd.DataFrame(results)
# Sort to match user's explicit preference visually
res_df['Bucket_Rank'] = res_df['Keeper Bucket'].map({'CORE': 0, 'EXPERIMENTAL': 1, 'DEMOTE': 2})
res_df = res_df.sort_values(by=['Bucket_Rank', 'Slice Wins vs Uncond', 'Max Rank Score'], ascending=[True, False, False])
res_df.drop(columns=['Bucket_Rank'], inplace=True)

def to_markdown_table(df):
    cols = df.columns.tolist()
    header = "| " + " | ".join(cols) + " |"
    separator = "| " + " | ".join(["---"] * len(cols)) + " |"
    rows = []
    for _, row in df.iterrows():
        rows.append("| " + " | ".join([str(x) for x in row.values]) + " |")
    return "\n".join([header, separator] + rows)

print(to_markdown_table(res_df))
