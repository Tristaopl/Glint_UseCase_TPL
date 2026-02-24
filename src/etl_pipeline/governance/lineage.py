"""
Data Lineage Tracking
Logs ETL run history and data transformations
"""
import json
import pandas as pd
from datetime import datetime
from pathlib import Path


class LineageTracker:
    """Tracks ETL execution lineage and logs"""

    def __init__(self, log_dir: Path = None):
        """
        Initialize lineage tracker
        
        Args:
            log_dir: Directory to store lineage logs (default: etl_pipeline/logs)
        """
        if log_dir is None:
            log_dir = Path(__file__).parent.parent / "logs"
        
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.runs = []

    def start_run(self, layer: str, table_name: str, source: str = None):
        """
        Log the start of a layer run
        
        Args:
            layer: Layer name (bronze, silver, gold)
            table_name: Name of table being processed
            source: Source of data (file path, database, etc.)
        """
        run = {
            "layer": layer,
            "table_name": table_name,
            "source": source,
            "start_time": datetime.now().isoformat(),
            "status": "running",
            "row_count_in": None,
            "row_count_out": None,
            "transformations": []
        }
        self.runs.append(run)
        return run

    def log_transformation(self, transformation_name: str, details: dict = None):
        """
        Log a transformation within the current run
        
        Args:
            transformation_name: Name of the transformation
            details: Additional details about the transformation
        """
        if self.runs:
            current_run = self.runs[-1]
            transformation = {
                "name": transformation_name,
                "timestamp": datetime.now().isoformat(),
                "details": details or {}
            }
            current_run["transformations"].append(transformation)

    def end_run(self, status: str = "success", row_count_in: int = None, 
                row_count_out: int = None, errors: list = None):
        """
        Log the end of a layer run
        
        Args:
            status: success, failed, or partial
            row_count_in: Number of rows at input
            row_count_out: Number of rows at output
            errors: List of errors that occurred
        """
        if self.runs:
            current_run = self.runs[-1]
            current_run["status"] = status
            current_run["end_time"] = datetime.now().isoformat()
            current_run["row_count_in"] = row_count_in
            current_run["row_count_out"] = row_count_out
            current_run["errors"] = errors or []

    def save_lineage_log(self):
        """Save lineage logs to JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.log_dir / f"lineage_{timestamp}.json"
        
        with open(log_file, 'w') as f:
            json.dump(self.runs, f, indent=2)
        
        print(f"📋 Lineage log saved to: {log_file}")
        return log_file

    def get_run_history(self, layer: str = None, table_name: str = None):
        """Get run history, optionally filtered by layer and table"""
        history = self.runs
        
        if layer:
            history = [r for r in history if r["layer"] == layer]
        if table_name:
            history = [r for r in history if r["table_name"] == table_name]
        
        return history

    def print_lineage_summary(self):
        """Print a summary of all runs"""
        print("\n" + "="*70)
        print("📋 LINEAGE SUMMARY")
        print("="*70)
        
        for i, run in enumerate(self.runs, 1):
            print(f"\n{i}. {run['layer'].upper()} → {run['table_name']}")
            print(f"   Status: {run['status']}")
            print(f"   Start: {run['start_time']}")
            if run['end_time']:
                print(f"   End:   {run['end_time']}")
            if run['row_count_in'] is not None:
                print(f"   Rows in:  {run['row_count_in']:>10}")
            if run['row_count_out'] is not None:
                print(f"   Rows out: {run['row_count_out']:>10}")
            if run['transformations']:
                print(f"   Transformations: {len(run['transformations'])}")
            if run['errors']:
                print(f"   ⚠️  Errors: {len(run['errors'])}")

    def export_lineage_report(self, output_file: str = None):
        """Export lineage as CSV report"""
        if output_file is None:
            output_file = self.log_dir / "lineage_report.csv"
        
        records = []
        for run in self.runs:
            records.append({
                "layer": run["layer"],
                "table": run["table_name"],
                "source": run["source"],
                "status": run["status"],
                "start_time": run["start_time"],
                "end_time": run["end_time"],
                "rows_in": run["row_count_in"],
                "rows_out": run["row_count_out"],
                "transformations": len(run["transformations"]),
                "errors": len(run["errors"])
            })
        
        df = pd.DataFrame(records)
        df.to_csv(output_file, index=False)
        
        print(f"📊 Lineage report saved to: {output_file}")
        return df
    def store_fact_table_checkpoint(self, fact_table_size: int = None,
                                   new_rows_processed: int = None,
                                   timestamp: datetime = None):
        """
        Store checkpoint for fact table incremental processing
        
        Args:
            fact_table_size: Total rows in fact table
            new_rows_processed: Number of new rows added
            timestamp: Timestamp of checkpoint
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        checkpoint = {
            "fact_table_size": fact_table_size,
            "new_rows_processed": new_rows_processed,
            "checkpoint_timestamp": timestamp.isoformat()
        }
        
        # Store in last run if exists
        if self.runs:
            self.runs[-1]["fact_checkpoint"] = checkpoint
        
        # Also save to dedicated file for recovery
        checkpoint_file = self.log_dir / "fact_table_checkpoint.json"
        checkpoint_list = []
        
        if checkpoint_file.exists():
            with open(checkpoint_file, 'r') as f:
                checkpoint_list = json.load(f)
        
        checkpoint_list.append(checkpoint)
        
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_list, f, indent=2)

    def get_fact_table_checkpoint(self):
        """
        Retrieve latest fact table checkpoint
        
        Returns:
            Dictionary with checkpoint info or None if no checkpoint exists
        """
        checkpoint_file = self.log_dir / "fact_table_checkpoint.json"
        
        if not checkpoint_file.exists():
            return None
        
        with open(checkpoint_file, 'r') as f:
            checkpoints = json.load(f)
        
        if checkpoints:
            return checkpoints[-1]  # Return latest
        
        return None