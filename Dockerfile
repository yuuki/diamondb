FROM golang:1.8.0

ENV USER root
RUN curl -sSL https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh > /wait-for-it.sh && \
    chmod +x /wait-for-it.sh

RUN go get github.com/golang/mock/mockgen && \
    go get golang.org/x/tools/cmd/goyacc && \
    curl https://glide.sh/get | sh
WORKDIR /go/src/github.com/yuuki/diamondb
ADD ./ /go/src/github.com/yuuki/diamondb
RUN make build
ENTRYPOINT ["/go/src/github.com/yuuki/diamondb/diamondb-server"]
