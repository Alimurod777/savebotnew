#!/usr/bin/env python3
# Test script to verify uvloop installation and performance

import asyncio
import time
import platform
import sys

def check_event_loop():
    """Check and report on the current event loop implementation."""
    print(f"Python version: {sys.version}")
    print(f"Platform: {platform.system()} {platform.release()}")
    
    # Try to import uvloop
    try:
        import uvloop
        print("uvloop is installed")
        
        # Check if we're on Windows
        if platform.system() == "Windows":
            print("Note: uvloop is not supported on Windows")
            print("Using default asyncio event loop")
        else:
            # Install uvloop
            try:
                uvloop.install()
                print("uvloop has been installed as the default event loop")
            except Exception as e:
                print(f"Failed to install uvloop: {e}")
    except ImportError:
        print("uvloop is not installed")
        
        # On Windows, use WindowsSelectorEventLoopPolicy
        if platform.system() == "Windows":
            if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
                print("Using WindowsSelectorEventLoopPolicy")
    
    # Get and print the current event loop (safe for Python 3.12+)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    loop_class = loop.__class__.__name__
    loop_module = loop.__class__.__module__
    print(f"Current event loop: {loop_module}.{loop_class}")
    
    return loop

async def dummy_task(duration):
    """A dummy task that sleeps for the given duration"""
    await asyncio.sleep(duration)
    return duration

async def run_benchmark():
    """Run a simple benchmark to test event loop performance"""
    print("\nRunning benchmark...")
    
    # Create many tasks
    start_time = time.time()
    tasks = []
    for i in range(1000):
        # Create tasks with varying sleep times (very short to simulate real workloads)
        tasks.append(asyncio.create_task(dummy_task(0.001)))
    
    # Wait for all tasks to complete
    results = await asyncio.gather(*tasks)
    end_time = time.time()
    
    print(f"Completed 1000 tasks in {end_time - start_time:.4f} seconds")
    return end_time - start_time

async def main():
    """Main test function"""
    # Check the event loop
    loop = check_event_loop()
    
    # Run the benchmark
    benchmark_time = await run_benchmark()
    
    print("\nTest completed successfully!")
    print(f"Benchmark time: {benchmark_time:.4f} seconds")
    
    # On Windows, show a message about uvloop
    if platform.system() == "Windows":
        print("\nNote: For production deployment, consider using a Linux environment")
        print("to take advantage of uvloop's performance benefits.")
        print("The bot has been configured to automatically use uvloop when available.")

if __name__ == "__main__":
    asyncio.run(main()) 