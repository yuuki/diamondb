FROM golang:1.8.0

ENV USER root
RUN go get github.com/golang/mock/mockgen && \
    curl https://glide.sh/get | sh
WORKDIR /go/src/github.com/yuuki/diamondb
ADD ./ /go/src/github.com/yuuki/diamondb
RUN make build
ENTRYPOINT ["/go/src/github.com/yuuki/diamondb/diamondb"]
