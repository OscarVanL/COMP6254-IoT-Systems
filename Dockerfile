FROM python:3

ADD . /workspace/mqtt
WORKDIR /workspace/mqtt

RUN pip install -r requirements.txt
CMD python -u ./main.py --coursework