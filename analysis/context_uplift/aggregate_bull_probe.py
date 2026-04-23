import pandas as pd
import os

base_dir = '/home/irene/Edge/analysis/bullish_trend_probe'
symbols = ['BTCUSDT', 'ETHUSDT']

all_data = []

for symbol in symbols:
    symbol_dir = os.path.join(base_dir, symbol)
    if not os.path.exists(symbol_dir): continue
    for slice_dir in os.listdir(symbol_dir):
        sb_path = os.path.join(symbol_dir, slice_dir, 'edge_scoreboard.parquet')
        if not os.path.exists(sb_path): continue
        
        df = pd.read_parquet(sb_path)
        df['symbol'] = symbol
        df['slice_label'] = slice_dir
        all_data.append(df)

if not all_data:
    print("No data found.")
    exit(0)

df_all = pd.concat(all_data, ignore_index=True)

# Focus on bullish_trend and positive_funding
df_bull = df_all[df_all['context_cell'] == 'bullish_trend']
df_pf = df_all[df_all['context_cell'] == 'positive_funding']

wins_vs_pf = 0
best_fold_depth = 0
best_winning_slice = "None"
best_rank_score = -1
winning_symbols = set()

for (symbol, slice_label), group in df_bull.groupby(['symbol', 'slice_label']):
    pf_group = df_pf[(df_pf['symbol'] == symbol) & (df_pf['slice_label'] == slice_label)]
    
    if group.empty: continue
    
    # Best rank in this slice for bullish_trend
    best_in_slice = group.sort_values('rank_score', ascending=False).iloc[0]
    bull_rank = best_in_slice['rank_score']
    
    # Best rank in this slice for PF
    pf_rank = pf_group['rank_score'].max() if not pf_group.empty else -1
    
    status = best_in_slice['status']
    survived = status in ['rankable_runtime_executable', 'rankable_thesis_eligible']
    
    if survived and bull_rank > pf_rank:
        wins_vs_pf += 1
        winning_symbols.add(symbol)
        if bull_rank > best_rank_score:
            best_rank_score = bull_rank
            best_winning_slice = f"{symbol} {slice_label}"
            
    current_fold_depth = best_in_slice['fold_valid_count']
    if current_fold_depth > best_fold_depth:
        best_fold_depth = current_fold_depth

print(f"Wins vs positive_funding: {wins_vs_pf}")
print(f"Winning Symbols: {', '.join(winning_symbols) if winning_symbols else 'None'}")
print(f"Best fold depth: {int(best_fold_depth)}")
print(f"Best winning slice: {best_winning_slice}")
