#!/usr/bin/env python3
"""
Model Download Script for OpenChat
Run this when you have a good internet connection to pre-download the model
"""

import os
import sys
import time
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForCausalLM
import torch

def download_model():
    """Download and cache the OpenChat model"""
    
    model_name = "openchat/openchat-3.5-1210"
    cache_dir = "./model_cache"
    
    print(f"🚀 Starting download of {model_name}")
    print(f"📁 Cache directory: {cache_dir}")
    
    # Create cache directory if it doesn't exist
    os.makedirs(cache_dir, exist_ok=True)
    
    try:
        # Test internet connection first
        print("🔍 Testing internet connection...")
        import requests
        response = requests.get("https://huggingface.co", timeout=10)
        if response.status_code != 200:
            raise Exception("Cannot reach Hugging Face")
        print("✅ Internet connection OK")
        
        # Download tokenizer first (smaller)
        print("\n📥 Downloading tokenizer...")
        start_time = time.time()
        
        tokenizer = AutoTokenizer.from_pretrained(
            model_name,
            cache_dir=cache_dir,
            force_download=False,  # Use existing cache if available
            resume_download=True   # Resume interrupted downloads
        )
        
        tokenizer_time = time.time() - start_time
        print(f"✅ Tokenizer downloaded in {tokenizer_time:.1f}s")
        
        # Download model (much larger)
        print("\n📥 Downloading model (this may take several minutes)...")
        start_time = time.time()
        
        model = AutoModelForCausalLM.from_pretrained(
            model_name,
            cache_dir=cache_dir,
            torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
            force_download=False,
            resume_download=True,
            low_cpu_mem_usage=True  # More memory efficient
        )
        
        model_time = time.time() - start_time
        print(f"✅ Model downloaded in {model_time:.1f}s")
        
        # Test the model works
        print("\n🧪 Testing model...")
        test_text = "Hello, how are you?"
        
        if tokenizer.pad_token is None:
            tokenizer.pad_token = tokenizer.eos_token
            
        input_ids = tokenizer.encode(test_text, return_tensors='pt')
        
        with torch.no_grad():
            outputs = model.generate(
                input_ids,
                max_length=input_ids.shape[1] + 10,
                temperature=0.7,
                do_sample=True,
                pad_token_id=tokenizer.pad_token_id
            )
            
        result = tokenizer.decode(outputs[0], skip_special_tokens=True)
        print(f"📝 Test generation: {result}")
        
        # Check cache size
        cache_path = Path(cache_dir)
        total_size = sum(f.stat().st_size for f in cache_path.rglob('*') if f.is_file())
        size_gb = total_size / (1024**3)
        
        print(f"\n✅ Download completed successfully!")
        print(f"📊 Total cache size: {size_gb:.2f} GB")
        print(f"📁 Cache location: {cache_path.absolute()}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Download failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Check your internet connection")
        print("2. Make sure you have enough disk space (need ~15GB)")
        print("3. Try again later if servers are busy")
        print("4. Consider using a VPN if you're behind a firewall")
        return False

def verify_model():
    """Verify that the model is properly cached"""
    
    model_name = "openchat/openchat-3.5-1210"
    cache_dir = "./model_cache"
    
    print(f"🔍 Verifying {model_name} in cache...")
    
    try:
        # Try to load from cache only
        tokenizer = AutoTokenizer.from_pretrained(
            model_name,
            cache_dir=cache_dir,
            local_files_only=True  # This will fail if not properly cached
        )
        
        model = AutoModelForCausalLM.from_pretrained(
            model_name,
            cache_dir=cache_dir,
            local_files_only=True,
            torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32
        )
        
        print("✅ Model is properly cached and can be loaded offline")
        return True
        
    except Exception as e:
        print(f"❌ Model not properly cached: {e}")
        return False

def clear_cache():
    """Clear the model cache"""
    
    cache_dir = "./model_cache"
    
    print(f"🗑️  Clearing cache directory: {cache_dir}")
    
    try:
        import shutil
        if os.path.exists(cache_dir):
            shutil.rmtree(cache_dir)
            print("✅ Cache cleared")
        else:
            print("ℹ️  Cache directory doesn't exist")
            
    except Exception as e:
        print(f"❌ Failed to clear cache: {e}")

def main():
    """Main function with menu"""
    
    print("🤖 OpenChat Model Manager")
    print("=" * 40)
    
    while True:
        print("\nOptions:")
        print("1. Download model")
        print("2. Verify model cache")
        print("3. Clear cache")
        print("4. Check disk space")
        print("5. Exit")
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == "1":
            print("\n" + "="*40)
            download_model()
            
        elif choice == "2":
            print("\n" + "="*40)
            verify_model()
            
        elif choice == "3":
            print("\n" + "="*40)
            confirm = input("Are you sure you want to clear the cache? (y/N): ")
            if confirm.lower() == 'y':
                clear_cache()
            else:
                print("Cancelled")
                
        elif choice == "4":
            print("\n" + "="*40)
            import shutil
            total, used, free = shutil.disk_usage(".")
            print(f"💾 Disk space:")
            print(f"   Total: {total // (1024**3)} GB")
            print(f"   Used:  {used // (1024**3)} GB") 
            print(f"   Free:  {free // (1024**3)} GB")
            print(f"   Recommended: At least 15 GB free for OpenChat")
            
        elif choice == "5":
            print("👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid choice")

if __name__ == "__main__":
    main()