from ubuntu:18.04
RUN apt-get update -y
RUN apt-get install -y python3.8 python3.8-distutils curl
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py &&\
 python3.8 get-pip.py
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1
ENV TZ=Asia/Shanghai
RUN export LC_ALL=C.UTF-8 && export LANG=C.UTF-8

RUN apt-get install -y less cron
ARG PIP_INSTALL='pip install -i http://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com'
RUN $PIP_INSTALL pandas==1.3.0
RUN $PIP_INSTALL sklearn==0.0

RUN DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata less
ENV data.path=/app/data
ENV log.path=/app/log

COPY requirements.txt /tmp/requirements.txt
RUN $PIP_INSTALL -r /tmp/requirements.txt

WORKDIR /app
RUN mkdir -p log data
COPY run.sh .
COPY src .
CMD ["/bin/bash", "run.sh"]
