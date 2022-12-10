FROM --platform=linux/amd64 ubuntu:focal

WORKDIR /files
COPY gensort /files/gensort
COPY populate_volume.sh /files/populate_volume.sh

CMD ["./populate_volume.sh"]
