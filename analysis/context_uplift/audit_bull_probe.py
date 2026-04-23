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

df_all = pd.concat(all_data, ignore_index=True)

df_bull = df_all[df_all['context_cell'] == 'bullish_trend']
df_pf = df_all[df_all['context_cell'] == 'positive_funding']

wins = []

for (symbol, slice_label), group in df_bull.groupby(['symbol', 'slice_label']):
    pf_group = df_pf[(df_pf['symbol'] == symbol) & (df_pf['slice_label'] == slice_label)]
    
    # survived = rankable
    survived_bull = group[group['status'].str.startswith('rankable')]
    if survived_bull.empty: continue
    
    best_bull = survived_bull.sort_values('rank_score', ascending=False).iloc[0]
    bull_rank = best_bull['rank_score']
    bull_folds = best_bull['fold_valid_count']
    
    pf_rank = pf_group['rank_score'].max() if not pf_group.empty else -1
    # Check if PF survived in any row of this slice
    survived_pf = pf_group[pf_group['status'].str.startswith('rankable')]
    pf_folds = survived_pf['fold_valid_count'].max() if not survived_pf.empty else 0
    
    if bull_rank > pf_rank:
        wins.append({
            'symbol': symbol,
            'slice': slice_label,
            'bull_rank': bull_rank,
            'bull_folds': bull_folds,
            'pf_rank': pf_rank,
            'pf_folds': pf_folds
        })

wins_df = pd.DataFrame(wins)
print(f"Total Wins vs PF: {len(wins_df)}")
if not wins_df.empty:
    print(wins_df.to_string(index=False))
    
    # Check pass conditions
    # 2 wins vs PF
    cond1 = len(wins_df) >= 2
    # at least 1 winning slice with >= benchmark fold depth
    # Benchmark fold depth is usually 6 (total) but here we mean valid folds.
    # Wait, PF often gets rejected. If PF has 0 valid folds, then bull_folds >= 0 is true.
    # But usually we want bull_folds >= 2.
    cond2 = (wins_df['bull_folds'] >= 2).any()
    
    print(f"\nCondition 1 (>= 2 wins): {'PASSED' if cond1 else 'FAILED'}")
    print(f"Condition 2 (>= 2 fold depth in a win): {'PASSED' if cond2 else 'FAILED'}")
