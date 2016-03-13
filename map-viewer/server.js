var http = require('http');
var fs = require('fs');

const PORT=8080;

var index = fs.readFileSync('index.html');

function handleRequest(request, response){
  if(request.url === '/data') {
    var data = fs.readFileSync('data.json');
    response.end(data);
  } else {
    response.end(index);
  }
}

var server = http.createServer(handleRequest);

server.listen(PORT, function(){
    console.log("Server listening on: http://localhost:%s", PORT);
});
