1- add mongo driver
  ```
  go get go.mongodb.org/mongo-driver/mongo
  ```
2- update vendor folder
  ```
  go mod vendor
  ```
3- use` go build` to build sources

4- Create Docker: docker build . -t k6:0.2

5- Start mongo docker image: cd test_NDE; docker-compose -f  docker-compose-mongo.yaml up -d

6- start test: docker run -v /mnt/d/workspace/src/github.com/loadimpact/k6/stats/example.js:/src/example.js -i k6:0.2  run /src/example.js --out mongo=mongodb://root:example@172.18.250.242:27017/k6 -v
