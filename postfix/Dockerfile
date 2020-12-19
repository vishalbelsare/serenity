FROM ubuntu:latest

RUN debconf-set-selections <<< "postfix postfix/mailname string cloudwall.io"
RUN debconf-set-selections <<< "postfix postfix/main_mailer_type string 'Internet Site'"
RUN apt-get update && apt-get install --assume-yes postfix

CMD ["postfix", "start-fg"]