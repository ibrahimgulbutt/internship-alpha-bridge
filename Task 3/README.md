//pre-req
you should have mongo db image running on port 27017

// make a docker image by this command

docker build -t Task3 .

// run with volume bind mapping in command line

docker run -p 3000:3000 -p 5000:5000 `           
  -v ${PWD}\backend:/app/backend`
-v ${PWD}\frontend:/app/frontend `
task3docker build -t Task3 .
