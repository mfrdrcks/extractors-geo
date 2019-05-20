FROM geopython/pycsw

ENV PYCSW_CONFIG=/etc/pycsw/pycsw.cfg

COPY entrypoint.py /usr/local/bin/entrypoint.py

WORKDIR /home/pycsw

USER pycsw

EXPOSE 8082

ENTRYPOINT [\
  "python3", \
  "/usr/local/bin/entrypoint.py" \
]
