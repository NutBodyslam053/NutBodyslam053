FROM python:alpine
WORKDIR /usr/local/app

# TODO - Copy the requirements.txt file into the working directory
COPY /requirements.txt /usr/local/app/

# TODO - Execute the "pip install -r requirements.txt" command to install the Python dependencies
RUN pip install -r requirements.txt

# TODO - Copy the contents of the src directory into the working directory
COPY /src /usr/local/app/

# TODO - Expose port 5000
EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0"]