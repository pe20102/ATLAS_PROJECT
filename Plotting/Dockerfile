#plotting dockerfile


FROM python:latest

#working directory set
WORKDIR /mydir

#copying plotting script
COPY Plotting/plotting.py /mydir/
COPY requirements.txt /mydir/

#installing the relevant packages needed
RUN pip install --no-cache-dir -r requirements.txt

#running the container
CMD ["python", "plotting.py"]