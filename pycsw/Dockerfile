FROM geopython/pycsw

RUN adduser -D -u 1000 pycsw

RUN pip3 install .

ENV PYCSW_CONFIG=/etc/pycsw/pycsw.cfg

RUN cp entrypoint.py /usr/local/bin/entrypoint.py

WORKDIR /home/pycsw

USER pycsw


EXPOSE 8082

ENTRYPOINT [\
  "python3", \
  "/usr/local/bin/entrypoint.py" \
]