import redis
import json
import uproot
import os
import time
import subprocess
import sys

# Add these lines to import infofile
current_script = os.path.dirname(os.path.realpath(__file__))
directory_after = os.path.dirname(current_script)
sys.path.append(directory_after)
import infofile

def run_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        print(f"Error executing {command}\nError message: {stderr.decode()}")
    else:
        print(stdout.decode())

def prepare_environment():
    print("Cleaning up Docker system...")
    run_command("docker system prune -f")

    run_command("docker network rm AtlasNetwork")

    print("Creating Docker network 'AtlasNetwork'...")
    run_command("docker network create AtlasNetwork")

    print("Removing existing files in 'process_data' directory...")
    run_command("del /f /q .\\process_data\\*")

    print("Building Docker images without using cache...")
    run_command("docker-compose build --no-cache")

def prepare_work_queue(r, samples, workers):
    # Clear existing queue
    r.delete("work_queue")
    print("Cleared existing work queue")
    
    tuple_path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"
    tasks_created = 0
    
    for sample in samples:
        print(f"Creating tasks for sample: {sample}")
        info_library = "Data/" if 'data' in sample else f"MC/mc_{infofile.infos[sample]['DSID']}."
        path = os.path.join(tuple_path, info_library + sample + ".4lep.root")
        
        try:
            with uproot.open(path + ":mini") as tree:
                entries = tree.num_entries
                
                #Specific handling for the ggH125_ZZ4lep sample
                if sample == 'ggH125_ZZ4lep' and workers > 2:
                    extra_workers = calculate_extra_workers(workers)
                    batch_size = entries // (workers + extra_workers)
                else:
                    batch_size = entries // workers
                
                remainder = entries % workers
                
                for i in range(workers):
                    start = i * batch_size
                    end = start + batch_size
                    
                    
                    
                    work_item = {
                        "sample": sample,
                        "start": start,
                        "end": end,
                        "worker_id": i + 1
                    }
                    
                    # Push to Redis queue and verify
                    r.lpush("work_queue", json.dumps(work_item))
                    tasks_created += 1
                    print(f"Created task {tasks_created}: {work_item}")
                    
        except Exception as e:
            print(f"Error creating tasks for {sample}: {e}")
    
    total_tasks = r.llen("work_queue")
    print(f"Total tasks in queue: {total_tasks}")
    return total_tasks

def calculate_extra_workers(workers):
    if workers <= 7:
        return (workers - 1) // 2
    else:
        return workers - 4

def main():
    try:
        workers = int(input("Enter the number of workers to use: "))
    except ValueError:
        print("Invalid input. Using default 2 workers.")
        workers = 2

    if input("Do you want to prepare the environment? (y/n): ").strip().lower() == 'y':
        prepare_environment()

    # Set environment variable for docker-compose
    os.environ['NUM_WORKERS'] = str(workers)

    # Start Redis first
    print("Starting Redis...")
    subprocess.run("docker-compose up -d redis", shell=True)
    time.sleep(5)  # Wait for Redis to be ready

    # Connect to Redis and prepare work queue
    print("Connecting to Redis and preparing work queue...")
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    # Define samples
    samples = ['data_A', 'data_B', 'data_C', 'data_D', 'Zee', 'Zmumu', 
               'ttbar_lep', 'llll', 'ggH125_ZZ4lep', 'VBFH125_ZZ4lep', 
               'WH125_ZZ4lep', 'ZH125_ZZ4lep']
    
    # Prepare work queue
    total_tasks = prepare_work_queue(r, samples, workers)
    print(f"Created {total_tasks} tasks in Redis queue")

    # Start the remaining containers
    print("Starting worker containers...")
    subprocess.run("docker-compose up -d", shell=True)

    # Monitor progress
    print("Processing data...")
    start_time = time.time()
    last_count = -1
    no_progress_count = 0
    
    while True:
        remaining_tasks = r.llen("work_queue")
        completed_tasks = total_tasks - remaining_tasks
        
        if remaining_tasks == last_count:
            no_progress_count += 1
            if no_progress_count > 12:
                print("Warning: No progress detected for 1 minute. Checking worker status...")
                subprocess.run("docker ps", shell=True)
                no_progress_count = 0
        else:
            no_progress_count = 0
        
        last_count = remaining_tasks
        
        if remaining_tasks == 0:
            print("All tasks completed!")
            break
        
        elapsed_time = time.time() - start_time
        print(f"Progress: {completed_tasks}/{total_tasks} tasks completed")
        print(f"Elapsed time: {elapsed_time:.1f} seconds")
        print(f"Remaining tasks: {remaining_tasks}")
        print("-" * 50)
        
        time.sleep(5)

if __name__ == "__main__":
    main() 