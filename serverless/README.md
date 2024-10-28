
### preequisits
1. install serverless
2. install plugin: `serverless plugin install -n serverless-deployment-bucket`
3. install docker

### deployment steps
1. Dockerfile
2. serverless.yml
3. start.sh
4. requirements.txt (`pip freeze > requirements.txt`)
5. Launch docker
6. deploy: `serverless deploy`

### Serverless Design
Lambda function 1. Process that Creates a Queue
    - check what is requested (list of etfs)
    - check what's been downloaded already (in s3)
    - create a queue of whatever is missing (etf + date combo)
    - how often does this run
    
Lambda function 2. Process that downloads data for each etf + date combo
    - gets triggered by lambda 1 running