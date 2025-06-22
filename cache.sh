#!/usr/bin/env python3
import os
import shutil
from pathlib import Path

def clean_caches():
    paths_to_clean = [
        Path.home() / ".cache" / "huggingface",
        Path("./model_cache"),
        Path.home() / ".cache" / "torch" / "transformers"
    ]
    
    total_freed = 0
    
    for path in paths_to_clean:
        if path.exists():
            # Calculate size before deletion
            size = sum(f.stat().st_size for f in path.rglob('*') if f.is_file())
            size_gb = size / (1024**3)
            
            print(f"🗑️ Deleting {path} ({size_gb:.1f} GB)")
            shutil.rmtree(path)
            total_freed += size
    
    print(f"✅ Total space freed: {total_freed / (1024**3):.1f} GB")

if __name__ == "__main__":
    clean_caches()