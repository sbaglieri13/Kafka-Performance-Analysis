import json
import pandas as pd
from pathlib import Path
from typing import Dict


class LatencyAnalyzer:
    def __init__(self, data_dir: str = "../data"):
        script_dir = Path(__file__).parent
        self.data_dir = (script_dir / data_dir).resolve()
        
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")
    
    def load_consumer_data(self) -> pd.DataFrame:
        """Carica tutti i file JSONL dei consumer"""
        consumer_files = list(self.data_dir.glob("consumer_*_messages_recap.jsonl"))
        
        if not consumer_files:
            print(f"No consumer files found in {self.data_dir}")
            return pd.DataFrame()
        
        print(f"Found {len(consumer_files)} consumer files")
        
        all_data = []
        for file_path in consumer_files:
            filename = file_path.stem  
            parts = filename.replace("_messages_recap", "").split("_")
            
            # Estrai container_name (es: consumer_1_g1)
            container_parts = []
            for i, part in enumerate(parts):
                if part.startswith("g") and i > 0:
                    container_parts.append(part)
                    break
                container_parts.append(part)
            
            container_name = "_".join(container_parts)
            
            with open(file_path, 'r') as f:
                for line in f:
                    record = json.loads(line)
                    record['container_name'] = container_name
                    all_data.append(record)
        
        return pd.DataFrame(all_data)
    
    def calculate_latency(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcola latency = receive_timestamp_ms - producer_send_timestamp_ms"""
        df['latency_ms'] = df['receive_timestamp_ms'] - df['producer_send_timestamp_ms']
        return df
    
    def aggregate_by_consumer(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggrega statistiche per singolo consumer e run_id"""
        stats = df.groupby(['run_id', 'container_name']).agg({
            'latency_ms': ['min', 'max', 'mean', 'count']
        }).reset_index()
        
        stats.columns = ['run_id', 'consumer', 'latency_min_ms', 'latency_max_ms', 
                        'latency_mean_ms', 'message_count']
        
        return stats
    
    def aggregate_by_group(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggrega statistiche per gruppo di consumer e run_id"""
        df['consumer_group'] = df['container_name'].apply(
            lambda x: f"group_{x.split('_g')[1][0]}" if '_g' in x else 'unknown'
        )
        
        stats = df.groupby(['run_id', 'consumer_group']).agg({
            'latency_ms': ['min', 'max', 'mean', 'count']
        }).reset_index()
        
        stats.columns = ['run_id', 'consumer_group', 'latency_min_ms', 
                        'latency_max_ms', 'latency_mean_ms', 'message_count']
        
        return stats
    
    def generate_report(self) -> Dict:
        """Genera il report con tutte le statistiche"""
        df = self.load_consumer_data()
        
        if df.empty:
            return {"error": "No consumer data found"}
        
        df = self.calculate_latency(df)
        unique_runs = sorted(df['run_id'].unique())
        
        reports = {}
        for current_run_id in unique_runs:
            run_df = df[df['run_id'] == current_run_id].copy()
            
            consumer_stats = self.aggregate_by_consumer(run_df)
            consumer_stats = consumer_stats[consumer_stats['run_id'] == current_run_id]
            
            group_stats = self.aggregate_by_group(run_df)
            group_stats = group_stats[group_stats['run_id'] == current_run_id]
            
            reports[current_run_id] = {
                "run_id": current_run_id,
                "by_consumer": consumer_stats.drop('run_id', axis=1).to_dict('records'),
                "by_group": group_stats.drop('run_id', axis=1).to_dict('records')
            }
        
        if len(reports) == 1:
            return list(reports.values())[0]
        
        return {
            "total_runs": len(reports),
            "runs": reports
        }
    
    def save_report(self, report: Dict):
        """Salva il report in latency_report.json"""
        output_file = self.data_dir / "latency_report.json"
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"Report saved in: {output_file}")


def main():
    try:
        analyzer = LatencyAnalyzer(data_dir="../data")
        
        print("Analyzing data...")
        report = analyzer.generate_report()
        
        print("Analysis completed")
        analyzer.save_report(report)
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
