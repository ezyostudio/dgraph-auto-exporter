# Build Stage 1
FROM node:24-alpine

WORKDIR /app

RUN --mount=type=cache,target=/root/.yarn YARN_CACHE_FOLDER=/root/.yarn yarn install

# Copy the entire project
COPY app.ts yarn.lock package.json ./

RUN corepack enable

# Install dependencies
RUN yarn install

RUN apk add --no-cache curl

# Change the port and host
ENV PORT=80
ENV HOST=0.0.0.0

EXPOSE 80

CMD ["yarn", "start"]
