FROM python:3.11
RUN pip install pandas
WORKDIR /usr/src/app
COPY pipeline.py ./
COPY mammal_input.csv ./
COPY mammal_output.csv ./
CMD [ "python", "pipeline.py", "-i", "mammal_input.csv", "-o", "mammal_output.csv" ]
#ENTRYPOINT [ “bash” ]