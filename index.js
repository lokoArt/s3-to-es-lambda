/*
 This is a lambda function that uploads ELB logs from S3 to Elasticsearch 
*/

const AWS = require('aws-sdk'); 
const zlib = require('zlib'); 
const readline = require('readline'); 
const path = require('path'); 
const stream = require('stream'); 

const elbLogRegexp = /([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) ([\d\.:]+) ([\d.]+) ([\d.]+) ([\d.]+) ([\d]+) ([\d]+) ([\d]+) ([^ ]+) ("[^"]+") ("[^"]+") ([^ ]+) ([^ ]+) ([^ ]+) ("[^"]+") ("[^"]+") ("[^"]+")/i;
const indexTimestamp = new Date().toISOString().replace(/\-/g, '.').replace(/T.+/, ''); 

const esDomain = {
    endpoint: 'xxx',
    region: 'xxx',
    index: 'elblogs-' + indexTimestamp, // adds a timestamp to index. Example: elblogs-2016.03.31
    doctype: 'elb-access-logs'
};

const endpoint = new AWS.Endpoint(esDomain.endpoint); 
const s3 = new AWS.S3(); 
var totalLogLines = 0; 
var numDocsAdded = 0;


function parseLogLine(line) {
	let result = line.match(elbLogRegexp)
	
	return {"schema" : result[1],
			"timestamp" : result[2],
			"elb" : result[3],
			"client_ip" : result[4],
			"http_response" : result[9],
			"url" : result[13],
			"agent" : result[14]}
}

/*
 * The AWS credentials are picked up from the environment.
 * They belong to the IAM role assigned to the Lambda function.
 * Since the ES requests are signed using these credentials,
 * make sure to apply a policy that permits ES domain operations
 * to the role.
 */ 
 
var creds = new AWS.EnvironmentCredentials('AWS'); 

/*
 * Get the log file from the given S3 bucket and key.  Parse it and add
 * each log record to the ES domain.
 */ 
function s3LogsToES(bucket, key, context) {
    // Note: The Lambda function should be configured to filter for .log.gz files
    // (as part of the Event Source "suffix" setting).
    
	let lineReader = readline.createInterface({ 
		input: s3.getObject({Bucket: bucket, Key: key}).createReadStream().pipe(zlib.createGunzip())
	});

	// Flow: S3 file stream -> ES
    lineReader
      .on('line', (line) => {
		    let logRecord = parseLogLine(line.toString());
			let serializedRecord = JSON.stringify(logRecord);
			totalLogLines ++;
		
            postDocumentToES(serializedRecord, context);
      });
      
    lineReader.on('error', (err) => {
        console.log(err);
        context.fail();
    });
}

/*
 * Add the given document to the ES domain.
 * If all records are successfully added, indicate success to lambda
 * (using the "context" parameter).
 */
function postDocumentToES(doc, context) {
    let req = new AWS.HttpRequest(endpoint);
    req.method = 'POST';
    req.path = path.join('/', esDomain.index, esDomain.doctype);
    req.region = esDomain.region;
    req.body = doc;
    req.headers['presigned-expires'] = false;
    req.headers['Host'] = endpoint.host;
    
	  // Sign the request (Sigv4)
    let signer = new AWS.Signers.V4(req, 'es');
    signer.addAuthorization(creds, new Date());

    let send = new AWS.NodeHttpClient();
    send.handleRequest(req, null, (httpResp) => {
        let body = '';
        httpResp.on('data', (chunk) => {
            body += chunk;
        });
        httpResp.on('end', (chunk) => {
            numDocsAdded ++;
            if (numDocsAdded === totalLogLines) {
				// Mark lambda success.  If not done so, it will be retried.
                console.log(numDocsAdded + ' log records added to ES.');
                context.succeed();
            }
        });
    }, (err) => {
        console.log('Error while adding a log: ' + err);
        console.log(numDocsAdded + 'of ' + totalLogLines + ' log records added to ES.');
        context.fail();
    });
}

exports.handler = function(event, context) {
    
    event.Records.forEach(function(record) {
        var bucket = record.s3.bucket.name;
        var objKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
        s3LogsToES(bucket, objKey, context);
    });
}
