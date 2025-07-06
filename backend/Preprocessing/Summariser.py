def summarize_text_column(series: pd.Series, max_examples: int = 3) -> str:
    examples = series.dropna().unique()[:max_examples]
    lengths = series.dropna().apply(len)
    summary = f"  Text length: min={lengths.min()}, max={lengths.max()}, avg={lengths.mean():.1f}\n"
    summary += f"  Sample entries:\n"
    for example in examples:
        summary += f"    - {example[:100]}{'...' if len(example) > 100 else ''}\n"
    return summary


def get_column_summary(df: pd.DataFrame, max_rows: int = 3) -> str:
    lines = []
    for col in df.columns:
        col_type = df[col].dtype
        total_missing = df[col].isnull().sum()
        summary = f"Column: {col} ({col_type})\n"
        summary += f"  Missing values: {total_missing} of {len(df)}\n"

        if pd.api.types.is_numeric_dtype(df[col]):
            summary += f"  Mean: {df[col].mean():.2f}\n"
            summary += f"  Std Dev: {df[col].std():.2f}\n"
            summary += f"  Min: {df[col].min()}, Max: {df[col].max()}\n"

        elif pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            if df[col].str.len().mean() > 30:  # Heuristic: if avg string length is large, it's text
                summary += summarize_text_column(df[col])
            else:
                top_vals = df[col].value_counts().head(max_rows).to_dict()
                summary += f"  Top {max_rows} frequent values: {top_vals}\n"

        elif pd.api.types.is_bool_dtype(df[col]):
            true_count = df[col].sum()
            summary += f"  True count: {true_count}, False count: {len(df) - true_count}\n"

        summary += "\n"
        lines.append(summary)
    return "".join(lines)
