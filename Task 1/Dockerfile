# Dockerfile for a Node.js application Means Os libaries
From node:22


WORKDIR /app

COPY package*.json ./

RUN npm install

COPY . .

EXPOSE 3000

CMD ["npm","start"]