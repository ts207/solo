import pandas as pd
import numpy as np
import os

base_dir = '/home/irene/Edge/analysis/context_uplift'
combinations = [
    ('BTCUSDT', 'full_2024'), ('BTCUSDT', 'h1_2024'), ('BTCUSDT', 'h2_2024'),
    ('ETHUSDT', 'full_2024'), ('ETHUSDT', 'h1_2024'), ('ETHUSDT', 'h2_2024')
]

all_rows = []
all_rejections = []

for symbol, slice_label in combinations:
    slice_dir = os.path.join(base_dir, symbol, slice_label)
    
    scoreboard_path = os.path.join(slice_dir, 'edge_scoreboard.parquet')
    universe_path = os.path.join(slice_dir, 'phase2_candidate_universe.parquet')
    metrics_path = os.path.join(slice_dir, 'phase2_candidate_fold_metrics.parquet')
    
    if not (os.path.exists(scoreboard_path) and os.path.exists(universe_path) and os.path.exists(metrics_path)):
        print(f"Skipping {symbol} {slice_label}, missing files")
        continue
        
    df_sb = pd.read_parquet(scoreboard_path)
    df_univ = pd.read_parquet(universe_path)
    df_metrics = pd.read_parquet(metrics_path)
    
    # Aggregate fold metrics per hypothesis
    df_metrics_valid = df_metrics[df_metrics['valid'] == True]
    agg_metrics = df_metrics_valid.groupby('hypothesis_id')['after_cost_expectancy_bps'].agg(
        mean_after_cost_expectancy_bps_valid_folds='mean',
        median_after_cost_expectancy_bps_valid_folds='median',
        worst_after_cost_expectancy_bps_valid_fold='min'
    ).reset_index()
    
    # Map hypothesis_id to source_cell_id which is cell_id in scoreboard
    univ_map = df_univ[['hypothesis_id', 'source_cell_id']].copy()
    univ_map.rename(columns={'source_cell_id': 'cell_id'}, inplace=True)
    
    metrics_map = pd.merge(univ_map, agg_metrics, on='hypothesis_id', how='left')
    
    # Merge metrics into scoreboard
    df_sb = pd.merge(df_sb, metrics_map, on='cell_id', how='left')
    
    df_sb['symbol'] = symbol
    df_sb['slice_label'] = slice_label
    df_sb['run_id'] = df_sb['source_scoreboard_run_id']
    df_sb['event_type'] = df_sb['event_atom']
    df_sb['best_template'] = df_sb['template']
    df_sb['best_direction'] = df_sb['direction']
    df_sb['best_horizon'] = df_sb['horizon']
    
    df_sb['survived'] = (
        (df_sb['fold_valid_count'] >= 1) & 
        (df_sb['status'].isin(['rankable_runtime_executable', 'rankable_thesis_eligible']))
    )
    
    df_sb['best_rejection_reason'] = df_sb['blocked_reason']
    
    # Sort criteria
    df_sb.sort_values(
        by=['survived', 'rank_score', 'fold_valid_count', 'mean_after_cost_expectancy_bps_valid_folds', 'n_events'],
        ascending=[False, False, False, False, False],
        inplace=True
    )
    
    # Group by bucket
    grouped = df_sb.groupby(['symbol', 'slice_label', 'event_type', 'context_cell'], sort=False).first().reset_index()
    
    # Also collect rejections (rows that didn't survive, for the second file)
    rejections = grouped[grouped['survived'] == False].copy()
    if not rejections.empty:
        all_rejections.append(rejections)
        
    all_rows.append(grouped)

if not all_rows:
    print("No data processed.")
    exit(0)

matrix = pd.concat(all_rows, ignore_index=True)

# Compute uplift columns vs unconditional FIRST
uncond = matrix[matrix['context_cell'] == 'unconditional'].copy()
uncond = uncond[['symbol', 'slice_label', 'event_type', 
                 'mean_after_cost_expectancy_bps_valid_folds', 
                 'rank_score', 'fold_valid_count']]

uncond.rename(columns={
    'mean_after_cost_expectancy_bps_valid_folds': 'uncond_mean_bps',
    'rank_score': 'uncond_rank_score',
    'fold_valid_count': 'uncond_fold_count'
}, inplace=True)

matrix = pd.merge(matrix, uncond, on=['symbol', 'slice_label', 'event_type'], how='left')

matrix['uplift_bps_vs_unconditional'] = np.where(
    matrix['context_cell'] != 'unconditional',
    matrix['mean_after_cost_expectancy_bps_valid_folds'] - matrix['uncond_mean_bps'],
    np.nan
)
matrix['uplift_rank_score_vs_unconditional'] = np.where(
    matrix['context_cell'] != 'unconditional',
    matrix['rank_score'] - matrix['uncond_rank_score'],
    np.nan
)
matrix['uplift_fold_valid_count_vs_unconditional'] = np.where(
    matrix['context_cell'] != 'unconditional',
    matrix['fold_valid_count'] - matrix['uncond_fold_count'],
    np.nan
)

# NOW null out score fields for non-survivors
mask = ~matrix['survived']
cols_to_null = ['rank_score', 'fold_valid_count', 'n_events', 
                'mean_after_cost_expectancy_bps_valid_folds', 
                'median_after_cost_expectancy_bps_valid_folds', 
                'worst_after_cost_expectancy_bps_valid_fold', 't_stat']
matrix.loc[mask, cols_to_null] = np.nan

# Select required columns
output_cols = [
    'symbol', 'slice_label', 'run_id', 'event_type', 'context_cell', 'survived',
    'best_template', 'best_direction', 'best_horizon', 'rank_score', 'fold_valid_count',
    'n_events', 'mean_after_cost_expectancy_bps_valid_folds',
    'median_after_cost_expectancy_bps_valid_folds', 'worst_after_cost_expectancy_bps_valid_fold',
    't_stat', 'best_rejection_reason',
    'uplift_bps_vs_unconditional', 'uplift_rank_score_vs_unconditional', 'uplift_fold_valid_count_vs_unconditional'
]

# Write outputs
matrix = matrix[output_cols]
matrix.to_parquet(os.path.join(base_dir, 'matrix.parquet'), index=False)
matrix.to_csv(os.path.join(base_dir, 'matrix.csv'), index=False)
print("Saved matrix.parquet and matrix.csv")

if all_rejections:
    rejections_df = pd.concat(all_rejections, ignore_index=True)
    rej_cols = [
        'symbol', 'slice_label', 'event_type', 'context_cell', 
        'best_rejection_reason', 'rank_score', 'fold_valid_count', 
        'mean_after_cost_expectancy_bps_valid_folds'
    ]
    rejections_df = rejections_df[rej_cols]
    rejections_df.rename(columns={
        'best_rejection_reason': 'top_rejection_reason',
        'rank_score': 'best_near_miss_rank_score',
        'fold_valid_count': 'best_near_miss_fold_count',
        'mean_after_cost_expectancy_bps_valid_folds': 'best_near_miss_valid_fold_mean_after_cost_bps'
    }, inplace=True)
    rejections_df.to_csv(os.path.join(base_dir, 'matrix_rejections.csv'), index=False)
    print("Saved matrix_rejections.csv")
