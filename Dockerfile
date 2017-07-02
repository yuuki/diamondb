FROM golang:1.8.3

ENV USER root
RUN curl -sSL https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh > /wait-for-it.sh && \
    chmod +x /wait-for-it.sh

RUN go get github.com/golang/mock/mockgen && \
    go get golang.org/x/tools/cmd/goyacc && \
    curl https://glide.sh/get | sh
ENV PKG github.com/yuuki/diamondb
WORKDIR /go/src/$PKG
ADD ./ /go/src/$PKG
RUN go build $PKG/cmd/...
ENTRYPOINT ["/go/src/github.com/yuuki/diamondb/diamondb-server"]
