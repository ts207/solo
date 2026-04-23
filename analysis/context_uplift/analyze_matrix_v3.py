import pandas as pd
import numpy as np

# Load data
matrix = pd.read_csv('/home/irene/Edge/analysis/context_uplift/matrix.csv')

# Get baseline metrics per slice for positive_funding (max rank score row per slice)
pf_df = matrix[(matrix['context_cell'] == 'positive_funding') & (matrix['survived'] == True)]
# Sort by rank score desc, then groupby slice_label and take first
pf_best_per_slice = pf_df.sort_values('rank_score', ascending=False).groupby('slice_label').first()
pf_slice_max_rank = pf_best_per_slice['rank_score'].to_dict()

# Compute metrics per context_cell
results = []
contexts = matrix[matrix['context_cell'] != 'unconditional']['context_cell'].unique()

for ctx in contexts:
    df_ctx = matrix[matrix['context_cell'] == ctx]
    survived_df = df_ctx[df_ctx['survived'] == True]
    
    surviving_slices_count = survived_df['slice_label'].nunique()
    
    # Wins vs unconditional: does the BEST row in this slice beat unconditional?
    # Wins vs PF: does the BEST row in this slice beat positive_funding's best row?
    slice_wins_vs_uncond = 0
    slice_wins_vs_pf = 0
    
    for slice_label, group in survived_df.groupby('slice_label'):
        best_in_slice = group.sort_values('rank_score', ascending=False).iloc[0]
        if best_in_slice['uplift_bps_vs_unconditional'] > 0:
            slice_wins_vs_uncond += 1
            
        pf_rank = pf_slice_max_rank.get(slice_label, -1)
        if best_in_slice['rank_score'] > pf_rank:
            slice_wins_vs_pf += 1

    max_fold_valid_count = survived_df['fold_valid_count'].max() if not survived_df.empty else 0
    max_rank_score = survived_df['rank_score'].max() if not survived_df.empty else 0
    median_uplift = df_ctx['uplift_bps_vs_unconditional'].median()
    
    if not survived_df.empty:
        best_row = survived_df.sort_values('rank_score', ascending=False).iloc[0]
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
        bucket = 'UNKNOWN'

    results.append({
        'Context Cell': ctx,
        'Surviving Slices': surviving_slices_count,
        'Slice Wins vs Uncond': slice_wins_vs_uncond,
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
