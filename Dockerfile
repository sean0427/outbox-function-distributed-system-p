FROM golang:1.19-alpine as build

WORKDIR /app

COPY . .
RUN go mod download
RUN CGO_ENABLED=0  go build -o /docker-exec ./handler/main.go


#####################################

FROM alpine:3.16.3

ENV APP_USER app
ENV APP_HOME /go/src/app

RUN addgroup -S $APP_USER && addcompany -S $APP_USER -G $APP_USER
RUN mkdir -p $APP_HOME


WORKDIR $APP_HOME

COPY --from=build /app $APP_HOME/app
RUN chown -R $APP_USER:$APP_USER $APP_HOME
RUN chmod -R 100 $APP_HOME

RUN touch /tmp/.lock 
# Write a health check for OpenFaaS here or in the HTTP server start-up

USER $APP_USER

CMD [ "./app" ]