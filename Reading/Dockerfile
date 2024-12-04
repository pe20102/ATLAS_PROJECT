#reading dockerfile
FROM python:latest

#working directory set
WORKDIR /mydir

#copying scripts
COPY Reading/reading.py /mydir/
COPY infofile.py /mydir/  
COPY requirements.txt /mydir/

#installing the relevant packages needed
RUN pip install --no-cache-dir -r requirements.txt

#running the container
CMD ["python", "reading.py"]
