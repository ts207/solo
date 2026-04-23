import pandas as pd
import os

base_dir = '/home/irene/Edge/analysis/chop_probe'
run_ids = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]

all_data = []

for rid in run_ids:
    sb_path = os.path.join(base_dir, rid, 'edge_scoreboard.parquet')
    if not os.path.exists(sb_path): continue
    
    df = pd.read_parquet(sb_path)
    df['run_id'] = rid
    
    # Extract year, slice, and family from run_id
    # CHOP_BTC_FPT_48B_2023_FULL
    parts = rid.split('_')
    df['family'] = parts[2]
    df['year'] = parts[4]
    df['slice'] = parts[5]
    
    all_data.append(df)

if not all_data:
    print("No data found.")
    exit(0)

df_all = pd.concat(all_data, ignore_index=True)

# Focus on chop and positive_funding
df_chop = df_all[df_all['context_cell'] == 'chop']
df_pf = df_all[df_all['context_cell'] == 'positive_funding']

# We need to compare chop vs pf per (year, slice, family)
# Wait, FPT and VOL were run separately. So we compare within each run.
wins_vs_pf = 0
best_fold_depth = 0
best_winning_slice = "None"
best_rank_score = -1
vol_win = False

for rid in run_ids:
    df_run = df_all[df_all['run_id'] == rid]
    chop_row = df_run[df_run['context_cell'] == 'chop']
    pf_row = df_run[df_run['context_cell'] == 'positive_funding']
    
    if chop_row.empty: continue
    
    chop_rank = chop_row['rank_score'].iloc[0]
    pf_rank = pf_row['rank_score'].iloc[0] if not pf_row.empty else -1
    
    # A win is defined as chop_rank > pf_rank AND survived (status is rankable)
    status = chop_row['status'].iloc[0]
    survived = status in ['rankable_runtime_executable', 'rankable_thesis_eligible']
    
    if survived and chop_rank > pf_rank:
        wins_vs_pf += 1
        family = chop_row['family'].iloc[0]
        if family == 'VOL':
            vol_win = True
            
        if chop_rank > best_rank_score:
            best_rank_score = chop_rank
            best_winning_slice = f"{chop_row['year'].iloc[0]}_{chop_row['slice'].iloc[0]} ({family})"
            
    current_fold_depth = chop_row['fold_valid_count'].iloc[0]
    if current_fold_depth > best_fold_depth:
        best_fold_depth = current_fold_depth

print(f"BTC wins vs positive_funding: {wins_vs_pf}")
print(f"Best fold depth: {int(best_fold_depth)}")
print(f"Best winning slice: {best_winning_slice}")
print(f"Second-family win (VOL_SHOCK): {'Yes' if vol_win else 'No'}")
