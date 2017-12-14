# S3 to ES Lambda Function

> This is a lambda function that allows to delivery AWS ELB logs from S3 to AWS ES

## Build Setup
  1. Clone the repository
  2. Set endpoint & region to your Elasticsearch in index.js
  3. Run npm install on your local machine
  4. Put everything into a zip folder
  5. Create a role for Lambda function with ES, S3 Permissions
  6. Create a lambda with the zip that you created, don't forget to set prefix to the folder with logs 
