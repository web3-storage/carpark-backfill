# Build the backfill worker
FROM node:18.15.0

WORKDIR /usr/src/app
COPY package*.json *.js ./
RUN npm install

COPY *.js *.json *.sh ./
CMD [ "npm", "start", "backfill", "--silent"]

EXPOSE 3000
HEALTHCHECK --interval=60s --timeout=1s --start-period=5s --retries=3 CMD [ "curl" "https://127.0.0.1:3000" "--fail" ]