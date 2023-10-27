FROM ubuntu:22.04
RUN apt update && apt install -y python3-pip
COPY . .
RUN pip3 install -r requirements.txt
EXPOSE 5000
ENTRYPOINT ["flask", "run"]
CMD ["--host=0.0.0.0"]