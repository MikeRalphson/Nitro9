var nitro = require('bbcparse/nitroSdk');

var key = process.env['MORPH_QUERY_KEY'];

var sql = '';
for (var a=2;a<process.argv.length;a++) {
	sql += (sql ? ' ' : '') + process.argv[a];
}

console.log(sql);

var query = nitro.newQuery();
query.add('key',key);
query.add('query',sql);

var path = '/MikeRalphson/Nitro9';

nitro.make_request('api.morph.io',path+'/data.json','',query,{"proto": "https"},function(obj){
	console.log(JSON.stringify(obj,null,2));
});
