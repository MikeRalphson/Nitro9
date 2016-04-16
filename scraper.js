// This is a template for a Node.js scraper on morph.io (https://morph.io)

//var request = require("request");
var sqlite3 = require("sqlite3").verbose();
var http = require('http');
var https = require('https');
var api = require('bbcparse/nitroApi/api.js');
var helper = require('bbcparse/apiHelper.js');
var nitro = require('bbcparse/nitroCommon.js');

var api_key = process.env.MORPH_API_KEY;
var gdb;
var domain = '/nitro/api';
var feed = '/programmes'
var index = 10000;

function iso8601durationToSeconds(input) {
	var reptms = /^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$/;
	var hours = 0, minutes = 0, seconds = 0, totalseconds;

	if (reptms.test(input)) {
		var matches = reptms.exec(input);
		if (matches[1]) hours = Number(matches[1]);
		if (matches[2]) minutes = Number(matches[2]);
		if (matches[3]) seconds = Number(matches[3]);
		totalseconds = hours * 3600  + minutes * 60 + seconds;
	}
	return totalseconds;
}

function toArray(item) {
	if (!(item instanceof Array)) {
		var newitem = [];
		if (item) {
			newitem.push(item);
		}
		return newitem;
	}
	else {
		return item;
	}
}

function initDatabase(callback) {
	// Set up sqlite database.
	var db = new sqlite3.Database("data.sqlite");
	
	var fields = [
		'#index',
		'type',
		'name',
		'pid',
		'available',
		'expires',
		'episode',
		'seriesnum',
		'episodenum',
		'versions',
		'duration',
		'desc',
		'channel',
		'categories',
		'thumbnail',
		'timeadded',
		'guidance',
		'web'
	];

	var fieldStr = '';
	for (var i in fields) {
		if (fieldStr) fieldStr += ',';
		fieldStr += '"'+fields[i]+'"' + ' TEXT';
	}

	db.serialize(function() {
		db.run('CREATE TABLE IF NOT EXISTS data ('+fieldStr+')');
		callback(db);
	});
}

function updateRow(db, prog) {
	// Insert some data.
	var statement = db.prepare("INSERT INTO data VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
	var value = [];
	for (var i in prog) {
		value.push(prog[i]);
	}
	//console.log(value);
	statement.run(value);
	statement.finalize();
}

function readRows(db) {
	// Read some data.
	db.each("SELECT rowid AS id, pid FROM data LIMIT 10", function(err, row) {
		if (!err) {
			console.log(row.id + ": " + row.pid);
		}
		else {
			console.log(err);
		}
	});
}

function persist(db,res) {
	for (var i in res.items) {
		var p = res.items[i];
		var prog = {};
		prog["#index"] = index++;
		prog.type = p.media_type ? (p.media_type == 'Audio' ? 'radio' : 'tv') : 'unknown'; //TODO podcast?
		prog.name = '';
		if (p.ancestor_titles) prog.name = p.ancestor_titles[0].title;
		prog.pid = p.pid;
		prog.available = p.updated_time;
		prog.expires = Math.floor(new Date()/1000.0)+2419200;;
		prog.episode = p.title;
		prog.episodenum = '';
		prog.seriesnum = '';
		prog.versions = 'default'; //TODO
		prog.duration = iso8601durationToSeconds(p.available_versions.version[0].duration);
		var desc = '';
		if (p.synopses) {
			if (p.synopses.short) desc = p.synopses.short;
		}
		prog.desc = desc;
		prog.channel = p.master_brand ? p.master_brand.mid : 'unknown';
		prog.categories = '';
		prog.thumbnail = 'http://ichef.bbci.co.uk/images/ic/160x90/'+p.pid+'.jpg';
		prog.timeadded = Math.floor(new Date()/1000.0);
		prog.guidance = p.has_guidance;
		prog.web = 'http://www.bbc.co.uk/programmes/'+p.pid;
		
		updateRow(db, prog);
	}
	return index;
}

var processResponse = function(obj) {
	var nextHref = '';
	if ((obj.nitro.pagination) && (obj.nitro.pagination.next)) {
		nextHref = obj.nitro.pagination.next.href;
		//console.log(nextHref);
	}
	var pageNo = obj.nitro.results.page;
	var top = obj.nitro.results.total;
	if (!top) {
		top = obj.nitro.results.more_than+1;
	}
	var last = Math.ceil(top/obj.nitro.results.page_size);
	process.stdout.write('.');

	var res = obj.nitro.results;
	persist(gdb,res);

	var dest = {};
	if (pageNo < last) {
		dest.path = domain+feed;
		dest.query = helper.queryFrom(nextHref,true);
		dest.callback = processResponse;
	}
	// if we need to go somewhere else, e.g. after all pages received set callback and/or path
	nitro.setReturn(dest);
	return true;
}

function run(db) {
	// Use bbcparse nitro SDK to read in pages.
	
	db.run('delete from "data"');
	gdb = db; //
	
	var host = 'programmes.api.bbc.com';
	var path = '/nitro/api/programmes';
	
	var query = helper.newQuery();
	query.add(api.fProgrammesMediaSet,'pc',true)
		.add(api.fProgrammesAvailabilityAvailable)
		.add(api.fProgrammesAvailabilityEntityTypeEpisode)
		//.add(api.fProgrammesMediaTypeAudio)
		.add(api.fProgrammesEntityTypeEpisode)
		.add(api.fProgrammesPaymentTypeFree)
		.add(api.mProgrammesAncestorTitles)
		.add(api.mProgrammesAvailability)
		.add(api.mProgrammesAvailableVersions);

	// parallelize the queries by 36 times
	var letters = '0123456789abcdefghijklmnopqrstuvwxyz';
	for (var l in letters) {
		if (letters.hasOwnProperty(l)) {
			var lQuery = query.clone();
			lQuery.add(api.fProgrammesInitialLetter,letters[l]);
			nitro.make_request(host,path,api_key,lQuery,{Accept:'application/json'},processResponse);
		}
	}
}

//----------------------------------------------------------------------------

initDatabase(run);

process.on('exit', function(code) {
	readRows(gdb);
	gdb.close();
});