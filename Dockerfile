FROM node:11-alpine

ARG STAGE=test
ENV STAGE $STAGE

RUN mkdir -p /app/

WORKDIR /app
COPY package.json ns_manager.js secrets.js /app/

RUN npm install
ENTRYPOINT [ "node", "ns_manager.js"]
