"""
DataFrame debug / inspection module.

Quick data inspection during ETL development and debugging.
Controlled via dag_run.conf: trigger with {"debug": true} from the Airflow UI.

Usage:
    from utils.debug import Debug

    # Create instance from Airflow context (in task functions)
    def task_load(**context):
        dbg = Debug(context)
        dbg.shape(df, "orders")       # only prints if debug=true
        dbg.inspect(df, "fact_table")  # only prints if debug=true

    # Static methods always print (for development/scripts)
    Debug.shape(df, "orders")
    Debug.inspect(df, "orders")
"""

import pandas as pd


class Debug:
    """DataFrame inspector. Instance mode is gated by dag_run.conf['debug']."""

    def __init__(self, context: dict = None):
        self.enabled = False
        if context:
            conf = getattr(context.get('dag_run'), 'conf', None) or {}
            self.enabled = conf.get('debug', False)

    def _run(self, method, *args, **kwargs):
        """Run a static method only if debug is enabled."""
        if self.enabled:
            return method(*args, **kwargs)
        if args:
            return args[0]
        return None

    # ── Instance methods (gated by dag_run.conf) ──

    def i_shape(self, df: pd.DataFrame, label: str = "") -> pd.DataFrame:
        return self._run(Debug.shape, df, label)

    def i_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._run(Debug.dtypes, df)

    def i_sample(self, df: pd.DataFrame, n: int = 5) -> pd.DataFrame:
        return self._run(Debug.sample, df, n)

    def i_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._run(Debug.nulls, df)

    def i_duplicates(self, df: pd.DataFrame, subset: list = None) -> pd.DataFrame:
        return self._run(Debug.duplicates, df, subset)

    def i_stats(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        return self._run(Debug.stats, df, column)

    def i_uniques(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        return self._run(Debug.uniques, df, column)

    def i_inspect(self, df: pd.DataFrame, label: str = "") -> pd.DataFrame:
        return self._run(Debug.inspect, df, label)

    # ── Static methods (always print) ──

    @staticmethod
    def shape(df: pd.DataFrame, label: str = "") -> pd.DataFrame:
        """Print DataFrame dimensions."""
        prefix = f"[{label}] " if label else ""
        print(f"{prefix}{df.shape[0]} rows x {df.shape[1]} columns")
        return df

    @staticmethod
    def dtypes(df: pd.DataFrame) -> pd.DataFrame:
        """Print column data types."""
        print("Column types:")
        for col in df.columns:
            print(f"  {col}: {df[col].dtype}")
        return df

    @staticmethod
    def sample(df: pd.DataFrame, n: int = 5) -> pd.DataFrame:
        """Print first N rows."""
        print(df.head(n).to_string())
        return df

    @staticmethod
    def nulls(df: pd.DataFrame) -> pd.DataFrame:
        """Print null counts per column (only columns with nulls)."""
        null_counts = df.isnull().sum()
        has_nulls = null_counts[null_counts > 0]
        if has_nulls.empty:
            print("No nulls found")
        else:
            print("Null counts:")
            for col, count in has_nulls.items():
                pct = count / len(df) * 100
                print(f"  {col}: {count} ({pct:.1f}%)")
        return df

    @staticmethod
    def duplicates(df: pd.DataFrame, subset: list = None) -> pd.DataFrame:
        """Print duplicate row count."""
        dupes = df.duplicated(subset=subset).sum()
        cols = subset if subset else "all columns"
        print(f"Duplicates on {cols}: {dupes}")
        return df

    @staticmethod
    def stats(df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Print basic stats for a numeric column."""
        if column not in df.columns:
            print(f"Column '{column}' not found")
            return df
        s = df[column]
        print(f"Stats for '{column}':")
        print(f"  min={s.min()}  max={s.max()}  mean={s.mean():.2f}  median={s.median():.2f}  std={s.std():.2f}")
        return df

    @staticmethod
    def uniques(df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Print unique value count and values for a column."""
        if column not in df.columns:
            print(f"Column '{column}' not found")
            return df
        unique_vals = df[column].nunique()
        print(f"Unique values in '{column}': {unique_vals}")
        if unique_vals <= 20:
            print(f"  Values: {df[column].unique().tolist()}")
        return df

    @staticmethod
    def inspect(df: pd.DataFrame, label: str = "") -> pd.DataFrame:
        """Run shape, dtypes, nulls, and sample together."""
        Debug.shape(df, label)
        Debug.dtypes(df)
        Debug.nulls(df)
        Debug.sample(df)
        return df
