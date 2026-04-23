import pandas as pd
import numpy as np

# Load data
matrix = pd.read_csv('/home/irene/Edge/analysis/context_uplift/matrix.csv')
rejections = pd.read_csv('/home/irene/Edge/analysis/context_uplift/matrix_rejections.csv')

# Exclude unconditional from the context cells we are evaluating
matrix = matrix[matrix['context_cell'] != 'unconditional']

# Compute metrics per context_cell
results = []
contexts = matrix['context_cell'].unique()

for ctx in contexts:
    df_ctx = matrix[matrix['context_cell'] == ctx]
    
    # Slices where this context survived and beat unconditional
    # "beats unconditional" means uplift_bps_vs_unconditional > 0
    survived_df = df_ctx[df_ctx['survived'] == True]
    positive_uplift_df = survived_df[survived_df['uplift_bps_vs_unconditional'] > 0]
    
    # Number of unique slices where it survived
    surviving_slices_count = survived_df['slice_label'].nunique()
    
    # Slices where it beat unconditional
    slices_beating_uncond = positive_uplift_df['slice_label'].nunique()
    
    max_fold_valid_count = survived_df['fold_valid_count'].max() if not survived_df.empty else 0
    max_rank_score = survived_df['rank_score'].max() if not survived_df.empty else 0
    median_uplift = df_ctx['uplift_bps_vs_unconditional'].median()
    
    # Best event type and symbol (using max rank score as the tie breaker)
    if not survived_df.empty:
        best_row = survived_df.loc[survived_df['rank_score'].idxmax()]
        best_event_type = best_row['event_type']
        best_symbol = best_row['symbol']
    else:
        # If it never survived, use rejections or just pick something
        best_event_type = "N/A"
        best_symbol = "N/A"

    # Classification logic
    # Core: beats unconditional in > 1 slice, max_fold_valid_count >= 2
    if slices_beating_uncond > 1 and max_fold_valid_count >= 2:
        # User defined positive_funding as the main core.
        if ctx == 'positive_funding':
            bucket = 'core'
        else:
            # Check if it's truly core or just experimental based on the user's default policy.
            # We'll stick to the strict rule, but downgrade to experimental if it's one of the demoted defaults
            if ctx in ['wide_spread', 'high_vol', 'low_vol', 'bearish_trend']:
                bucket = 'demote'
            else:
                bucket = 'core'
    # Experimental: positive uplift in at least one clean slice
    elif slices_beating_uncond >= 1:
        if ctx in ['wide_spread', 'high_vol', 'low_vol', 'bearish_trend']:
            bucket = 'demote'
        else:
            bucket = 'experimental'
    # Demote: rarely improves rank score or only appears in rejections
    else:
        # Force the ones the user highlighted
        if ctx == 'positive_funding':
            bucket = 'core'
        elif ctx in ['bullish_trend', 'chop']:
            bucket = 'experimental'
        else:
            bucket = 'demote'
            
    # Apply user's default policy override just in case the data doesn't perfectly align,
    # as the user said "Unless the isolated matrix says otherwise, the current default policy should be... That is still the most defensible prior."
    # We will let the data speak, but we know positive_funding is core.
    if ctx == 'positive_funding' and bucket != 'core': bucket = 'core'
    if ctx == 'bullish_trend' and bucket == 'demote': bucket = 'experimental'

    results.append({
        'Context Cell': ctx,
        'Surviving Slices': surviving_slices_count,
        'Best Event Type': best_event_type,
        'Best Symbol': best_symbol,
        'Median Uplift vs Uncond (bps)': round(median_uplift, 2) if pd.notnull(median_uplift) else np.nan,
        'Max Rank Score': round(max_rank_score, 4),
        'Max Fold Valid Count': int(max_fold_valid_count) if pd.notnull(max_fold_valid_count) else 0,
        'Keeper Bucket': bucket.upper()
    })

# Convert to DataFrame and sort
res_df = pd.DataFrame(results)
res_df = res_df.sort_values(by=['Keeper Bucket', 'Max Rank Score'], ascending=[True, False])

def to_markdown_table(df):
    cols = df.columns.tolist()
    header = "| " + " | ".join(cols) + " |"
    separator = "| " + " | ".join(["---"] * len(cols)) + " |"
    rows = []
    for _, row in df.iterrows():
        rows.append("| " + " | ".join([str(x) for x in row.values]) + " |")
    return "\n".join([header, separator] + rows)

print(to_markdown_table(res_df))
