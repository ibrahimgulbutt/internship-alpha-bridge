FROM node:20

WORKDIR /app

# Set environment to development so devDependencies are installed
ENV NODE_ENV=development

COPY package*.json ./

RUN npm install

COPY . .

EXPOSE 5000

CMD ["./node_modules/.bin/nodemon", "index.js"]
