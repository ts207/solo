import pandas as pd
import numpy as np

matrix = pd.read_csv('/home/irene/Edge/analysis/context_uplift_expanded/matrix_expanded.csv')

# Only focus on the current core/experimental surface
allowed_contexts = ['positive_funding', 'chop', 'negative_funding', 'bullish_trend']
matrix = matrix[matrix['context_cell'].isin(allowed_contexts) | (matrix['context_cell'] == 'unconditional')]

# Get baseline metrics per slice for positive_funding
pf_df = matrix[(matrix['context_cell'] == 'positive_funding') & (matrix['survived'] == True)]
pf_slice_max_rank = pf_df.sort_values('rank_score', ascending=False).groupby(['symbol', 'slice_label'])['rank_score'].first().to_dict()

results = []
contexts = matrix[matrix['context_cell'] != 'unconditional']['context_cell'].unique()

for ctx in contexts:
    df_ctx = matrix[matrix['context_cell'] == ctx]
    survived_df = df_ctx[df_ctx['survived'] == True]
    
    surviving_slices_count = len(survived_df.groupby(['symbol', 'slice_label']))
    
    slice_wins_vs_uncond = 0
    slice_wins_vs_pf = 0
    winning_symbols_vs_pf = set()
    
    for (symbol, slice_label), group in survived_df.groupby(['symbol', 'slice_label']):
        best_in_slice = group.sort_values('rank_score', ascending=False).iloc[0]
        if best_in_slice['uplift_bps_vs_unconditional'] > 0:
            slice_wins_vs_uncond += 1
            
        pf_rank = pf_slice_max_rank.get((symbol, slice_label), -1)
        if best_in_slice['rank_score'] > pf_rank:
            slice_wins_vs_pf += 1
            winning_symbols_vs_pf.add(symbol)

    max_fold_valid_count = survived_df['fold_valid_count'].max() if not survived_df.empty else 0
    max_rank_score = survived_df['rank_score'].max() if not survived_df.empty else 0
    
    # Promotion rules
    if ctx == 'positive_funding':
        bucket = 'CORE (Benchmark)'
    elif slice_wins_vs_pf >= 2 and max_fold_valid_count >= 2 and len(winning_symbols_vs_pf) >= 2:
        bucket = 'CORE'
    elif slice_wins_vs_uncond >= 1:
        bucket = 'EXPERIMENTAL'
    else:
        bucket = 'DEMOTE'

    results.append({
        'Context Cell': ctx,
        'Surviving Slices (Max 15)': surviving_slices_count,
        'Slice Wins vs Uncond': slice_wins_vs_uncond,
        'Slice Wins vs PF': slice_wins_vs_pf,
        'Winning Symbols vs PF': len(winning_symbols_vs_pf),
        'Max Rank Score': round(max_rank_score, 4),
        'Max Fold Valid Count': int(max_fold_valid_count) if pd.notnull(max_fold_valid_count) else 0,
        'Keeper Bucket': bucket
    })

res_df = pd.DataFrame(results)

def bucket_sort(b):
    if 'CORE' in b: return 0
    if 'EXPERIMENTAL' in b: return 1
    return 2

res_df['Bucket_Rank'] = res_df['Keeper Bucket'].apply(bucket_sort)
res_df = res_df.sort_values(by=['Bucket_Rank', 'Slice Wins vs PF', 'Slice Wins vs Uncond', 'Max Rank Score'], ascending=[True, False, False, False])
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
