# Base image
FROM python:latest

# Working directory inside the container
WORKDIR /mydir

# Copy the manager script and requirements
COPY Manager/manager.py /mydir/ 
COPY requirements.txt /mydir/ 

# Install required Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Command to run the manager script
CMD ["python", "manager.py"]
