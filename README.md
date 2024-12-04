# Analysis of ATLAS OPEN DATA Using Cloud Technologies
A program for aggregating and visualising an ATLAS data for the Higgs Boson was implemented using cloud technologies such as Docker and Redis.

### Installation and Usage

Docker Desktop and Redis must already be installed on your computing system for the program to work.

1. Clone the repository
2. Check that the repository is of the following structure and that no files are missing:
`````
ATLAS_PROJECT/
├── Manager/
│   ├── Dockerfile
│   └── manager.py
├── Plotting/
│   ├── Dockerfile
│   └── plotting.py
├── Reading/
│   ├── Dockerfile
│   └── reading.py
├── docker-compose.yml
├── infofile.py
└── requirements.txt
`````
3. To run the program: Navigate to the repository in the command prompt.
4. Then use the command:
```bash
python Manager/manager.py
```
5. Specify the number of workers (Recommend a maximum of 1 per CPU core)
6. Choose whether to prepare the Docker environment
7. The program should start to run.
8. Once completed there should be a new directory named "process_data" containing the aggregated data and plots.

### Erroneous Circumstances
The program will display the number of completed tasks every 5 seconds until completion. I have found that occasionally, the number of tasks completed would be stuck at 0.
I am unsure of the exact cause of this error but so far it would seem like a Redis port error which I have been able to resolve by changing the Redis port in the yml file (e.g., from 6780 to 6781).
If you run into any errors like this please feel free to contact me at **-email** pe20102@bristol.ac.uk
