FROM golang:1.20.2-alpine as build

WORKDIR /app

COPY . .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o app ./cmd/main.go


#####################################

FROM alpine:3.17

ENV APP_USER app
ENV APP_HOME /go/src/app

RUN addgroup -S $APP_USER && adduser -S $APP_USER -G $APP_USER
RUN mkdir -p $APP_HOME


WORKDIR $APP_HOME

COPY --from=build /app $APP_HOME
RUN chown -R $APP_USER:$APP_USER $APP_HOME
RUN chmod -R 100 $APP_HOME
EXPOSE 8080
USER $APP_USER

CMD [ "./app" ]