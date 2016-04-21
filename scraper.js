// This is a Node.js scraper on morph.io (https://morph.io)

var sqlite3 = require("sqlite3").verbose();
var api = require('bbcparse/nitroApi/api.js');
var helper = require('bbcparse/apiHelper.js');
var nitro = require('bbcparse/nitroCommon.js');

var api_key = process.env.MORPH_API_KEY;
var production = process.env.NPM_CONFIG_PRODUCTION; // true if running on morph.io
var rebuild = process.env.MORPH_REBUILD;
var gdb;
var host = 'programmes.api.bbc.com';
var index = 100000;
var rows = 0;
var increment = production ? 1000 :  100;
var target = increment;
var abort = false;

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
		'web',
		'vpids'
	];

	var fieldStr = '';
	for (var i in fields) {
		if (fieldStr) fieldStr += ',';
		fieldStr += '"'+fields[i]+'"' + ' TEXT';
		if (i==3) fieldStr += ' PRIMARY KEY';
	}

	console.log('Prepping database...');
	db.serialize(function() {
		db.run('CREATE TABLE IF NOT EXISTS data ('+fieldStr+')');
		callback(db);
	});
}

function updateRows(db, progs) {
	// Insert some data.
	// index numbers may now not be unique, so we reset them to rowid+base at the end
	var statement = db.prepare("INSERT OR REPLACE INTO data VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
	for (var p in progs) {
		var prog = progs[p];
		var value = [];
		for (var i in prog) {
			value.push(prog[i]);
		}
		statement.run(value);
	}
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

function cleanup(callback) {
	console.log('\nCleaning up');
	gdb.serialize(function(){
		gdb.run('UPDATE data SET "#index" = rowid+100000');
		gdb.run('SELECT 0');
	});
	if (callback) callback();
}

function persist(db,res,parent) {

	var progs = [];

	for (var i in res.items) {
		var p = res.items[i];

		if (p.item_type != 'episode') {
			if ((p.item_type == 'brand') || (p.item_type == 'series')) {
				var query = helper.newQuery();
				query.add(api.fProgrammesMediaSet,'pc',true)
					.add(api.fProgrammesAvailabilityAvailable)
					.add(api.fProgrammesAvailabilityEntityTypeEpisode)
					.add(api.fProgrammesPaymentTypeFree)
					.add(api.fProgrammesDescendantsOf,p.pid)
					.add(api.mProgrammesGenreGroupings)
					.add(api.mProgrammesAncestorTitles)
					.add(api.mProgrammesAvailability)
					.add(api.mProgrammesAvailableVersions);
				var settings = {};
				settings.payload = p; // gets passed back into the callback
				if (!abort) nitro.make_request(host,api.nitroProgrammes,api_key,query,settings,processResponse);
			}
			// ignore clips
		}
		else {
			var prog = {};
			prog["#index"] = index++;
			prog.type = p.media_type ? (p.media_type == 'Audio' ? 'radio' : 'tv') : 'radio'; // all unknown seem to be R3/R4/WS
			prog.name = p.ancestor_titles ? p.ancestor_titles[0].title : p.title;
			prog.pid = p.pid;
			prog.available = p.updated_time;
			prog.expires = Math.floor(new Date()/1000.0)+(30*24*60*60);
			prog.episode = p.title;
			prog.seriesnum = (parent && parent.episode_of && parent.episode_of.position ? parent.episode_of.position : '');
			prog.episodenum = (p.episode_of && p.episode_of.position ? p.episode_of.position : '');
			prog.versions = 'default';
			prog.duration = '';
			if ((p.available_versions) && (p.available_versions.version) && (p.available_versions.version.length>0)) {
				prog.duration = helper.iso8601durationToSeconds(p.available_versions.version[0].duration);
			}
			var desc = '';
			if (p.synopses) {
				if (p.synopses.short) desc = p.synopses.short
				else if (p.synopses.medium) desc = p.synopses.medium
				else if (p.synopses.long) desc = p.synopses.long;
			}
			prog.desc = desc;
			prog.channel = p.master_brand ? p.master_brand.mid : 'unknown';
			prog.categories = '';
			prog.thumbnail = 'http://ichef.bbci.co.uk/images/ic/160x90/'+p.pid+'.jpg';
			prog.timeadded = Math.floor(new Date()/1000.0);
			prog.guidance = '';
			prog.web = 'http://www.bbc.co.uk/programmes/'+p.pid;
			prog.vpids = '';

			if (p.genre_groupings && p.genre_groupings.genre_group) {
				for (var gg=0;gg<p.genre_groupings.genre_group.length;gg++) {
					var genre_group = p.genre_groupings.genre_group[gg];
					if (genre_group.genres && genre_group.genres.genre) {
						for (var g=0;g<genre_group.genres.genre.length;g++) {
							var genre = genre_group.genres.genre[g];
							prog.categories += (prog.categories ? ',' : '')+genre["$"];
						}
					}
				}
			}

			if (p.programme_formats && p.programme_formats.format) {
				for (var f=0;f<p.programme_formats.format.length;f++) {
					var format = p.programme_formats.format[f]
					prog.categories += (prog.categories ? ',' : '')+format["$"];
				}
			}

			if (p.available_versions && p.available_versions.version) {
				prog.versions = '';
				for (var v=0;v<p.available_versions.version.length;v++) {
					var version = p.available_versions.version[v];
					if (version.pid) prog.vpids += (prog.vpids ? ',' : '') + version.pid;

					if (version.warnings && version.warnings.warning_texts && version.warnings.warning_texts.warning_text) {
						for (var w=0;w<version.warnings.warning_texts.warning_text.length;w++) {
							var warning = version.warnings.warning_texts.warning_text[w];
							if (prog.guidance == '') prog.guidance = warning["$"];
						}
					}

					if (version.types && version.types.type) {
						for (var t=0;t<version.types.type.length;t++) {
							var type = version.types.type[t];
							if (type == 'Original') type = 'default';
							type = type.toLocaleLowerCase();
							prog.versions += (prog.versions ? ',' : '') + type;
						}
					}

					if (version.availabilities && version.availabilities.availability) {
						for (var a=0;a<version.availabilities.availability.length;a++) {
							var availability = version.availabilities.availability[a];
							if (availability.scheduled_start) {
								prog.available = availability.scheduled_start;
							}
							if (availability.scheduled_end) {
								prog.expires = Math.floor(new Date(availability.scheduled_end)/1000.0);
							}
						}
					}
				}
			}

			if ((prog.available == p.updated_time) && parent && parent.scheduled_time) {
				prog.available = parent.scheduled_time.start;
			}
			if ((prog.available == p.updated_time) && parent && parent.published_time) {
				prog.available = parent.published_time.start;
			}

			progs.push(prog);

		}
	}
	updateRows(db, progs);
	rows += progs.length;
	return index;
}

var processResponse = function(obj,payload) {
	var nextHref = '';
	if ((obj.nitro.pagination) && (obj.nitro.pagination.next)) {
		nextHref = obj.nitro.pagination.next.href;
	}
	var pageNo = obj.nitro.results.page;
	var top = obj.nitro.results.total;
	if (!top) {
		top = obj.nitro.results.more_than+1;
	}
	var last = Math.ceil(top/obj.nitro.results.page_size);

	var res = obj.nitro.results;
	if ((rows >= target) || abort) {
		process.stdout.write('\rIn-flight: '+nitro.getRequests()+' Rate-limit: '+nitro.getRateLimitEvents()+' Rows: '+rows+' ');
		target += increment; // doesn't matter about target if we're aborting
	}

	persist(gdb,res,payload);

	var dest = {};
	if ((pageNo < last) && (!abort)) {
		dest.path = api.nitroProgrammes;
		dest.query = helper.queryFrom(nextHref,true);
		dest.callback = processResponse;
	}
	// if we need to go somewhere else, e.g. after all pages received set callback and/or path
	nitro.setReturn(dest);

	if (nitro.getRequests()<=1) {
		cleanup();
	}

	return true;
}

function processSchedule(obj) {
	//console.log(JSON.stringify(obj,null,2));
	for (var i in obj.nitro.results.items) {
		var bcast = obj.nitro.results.items[i];

		var pid;

		for (var b in bcast.broadcast_of) {
			var of = bcast.broadcast_of[b];
			if (of.result_type == 'episode') {
				pid = of.pid;
			}
		}
		for (var w in bcast.window_of) {
			var of = bcast.window_of[w];
			if (of.type == 'episode') {
				pid = of.pid;
			}
		}

		if (pid) {
			var query = helper.newQuery();
			query.add(api.fProgrammesPid,pid,true)
				.add(api.fProgrammesAvailabilityAvailable)
				.add(api.fProgrammesAvailabilityPending)
				.add(api.mProgrammesGenreGroupings)
				.add(api.mProgrammesAncestorTitles)
				.add(api.mProgrammesAvailability)
				.add(api.mProgrammesAvailableVersions);
			if (!abort) {
				var settings = {};
				settings.payload = bcast;
				nitro.make_request(host,api.nitroProgrammes,api_key,query,settings,processResponse);
			}
		}
	}

	var nextHref = '';
	if ((obj.nitro.pagination) && (obj.nitro.pagination.next)) {
		nextHref = obj.nitro.pagination.next.href;
	}
	var dest = {};
	if (nextHref) {
		dest.path = api.nitroSchedules;
		dest.query = helper.queryFrom(nextHref,true);
		dest.callback = processSchedule;
	}
	nitro.setReturn(dest);
	return true;
}

function buildScheduleFrom(start) {
	//increment = 10;
	//target = 10;
	var now = new Date().toISOString();
	console.log(start+' to '+now);
	var query = helper.newQuery();
	query.add(api.fSchedulesStartFrom,start,true)
		.add(api.fSchedulesEndTo,now);
	nitro.make_request(host,api.nitroSchedules,api_key,query,{},processSchedule);
}

function run(db) {
	// Use bbcparse nitro SDK to read in pages.

	gdb = db; // to save having to pass it around callbacks

	nitro.ping(host,api_key,{},function(obj){
		if (obj.nitro.results) {
			console.log('Removing expired items...');
			db.run('DELETE FROM "data" WHERE expires < strftime("%s","now")');

			db.each("SELECT max(available) AS start FROM data", function(err, row) {
				if (err) {
					console.log(err);
				}
				else if (row.start && !rebuild) {
					buildScheduleFrom(row.start);
				}
				else {
					var query = helper.newQuery();
					query.add(api.fProgrammesMediaSet,'pc',true)
						.add(api.fProgrammesAvailabilityAvailable)
						.add(api.fProgrammesAvailabilityEntityTypeEpisode)
						.add(api.fProgrammesPaymentTypeFree)
						.add(api.mProgrammesGenreGroupings)
						.add(api.mProgrammesAncestorTitles)
						.add(api.mProgrammesAvailability)
						.add(api.mProgrammesAvailableVersions);

					console.log('Firing initial queries...');
					// parallelize the queries by 36 times
					var letters = '0123456789abcdefghijklmnopqrstuvwxyz';
					for (var l in letters) {
						if (letters.hasOwnProperty(l)) {
							var lQuery = query.clone();
							lQuery.add(api.fProgrammesInitialLetterStrict,letters[l]);
							nitro.make_request(host,api.nitroProgrammes,api_key,lQuery,{},processResponse);
						}
					}
				}
			});
		}
		else {
			console.log('Could not nitro.ping '+host);
		}
	});
}

//----------------------------------------------------------------------------

process.on('exit', function(code) {
	gdb.close();
	console.log('Exiting with '+rows+' rows processed');
});

process.on('SIGINT', function () {
	console.log('\nProcess killed, setting abort flag...');
	process.exitCode = 2;
	abort = true;
});

initDatabase(run);
