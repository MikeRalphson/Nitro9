// This is a Node.js scraper on morph.io (https://morph.io)

var sqlite3 = require("sqlite3").verbose();
var api = require('bbcparse/nitroApi/api.js');
var helper = require('bbcparse/apiHelper.js');
var nitro = require('bbcparse/nitroCommon.js');

var api_key = process.env.MORPH_API_KEY;
var gdb;
var domain = '/nitro/api';
var feed = '/programmes'
var index = 10000;

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
	}

	db.serialize(function() {
		db.run('CREATE TABLE IF NOT EXISTS data ('+fieldStr+')');
		callback(db);
	});
}

function updateRow(db, prog) {
	// Insert some data.
	var statement = db.prepare("INSERT INTO data VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
	var value = [];
	for (var i in prog) {
		value.push(prog[i]);
	}
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
		prog.type = p.media_type ? (p.media_type == 'Audio' ? 'radio' : 'tv') : 'unknown';
		prog.name = p.ancestor_titles ? p.ancestor_titles[0].title : p.title;
		prog.pid = p.pid;
		prog.available = p.updated_time;
		prog.expires = Math.floor(new Date()/1000.0)+2419200;;
		prog.episode = p.title;
		prog.episodenum = (p.episode_of && p.episode_of.position ? p.episode_of.position : '');
		prog.seriesnum = '';
		prog.versions = 'default';
		prog.duration = helper.iso8601durationToSeconds(p.available_versions.version[0].duration);
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
			}
		}

		updateRow(db, prog);
	}
	return index;
}

var processResponse = function(obj) {
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

	gdb = db; // to save having to pass it around callbacks
	var host = 'programmes.api.bbc.com';
	var path = '/nitro/api/programmes';

	nitro.ping(host,api_key,{},function(obj){
		if (obj.nitro.results) {
			db.run('delete from "data"');

			var query = helper.newQuery();
			query.add(api.fProgrammesMediaSet,'pc',true)
				.add(api.fProgrammesAvailabilityAvailable)
				.add(api.fProgrammesAvailabilityEntityTypeEpisode)
				.add(api.fProgrammesEntityTypeEpisode)
				.add(api.fProgrammesPaymentTypeFree)
				.add(api.mProgrammesGenreGroupings)
				.add(api.mProgrammesAncestorTitles)
				.add(api.mProgrammesAvailability)
				.add(api.mProgrammesAvailableVersions);

			// parallelize the queries by 36 times
			var letters = '0123456789abcdefghijklmnopqrstuvwxyz';
			for (var l in letters) {
				if (letters.hasOwnProperty(l)) {
					var lQuery = query.clone();
					lQuery.add(api.fProgrammesInitialLetterStrict,letters[l]);
					nitro.make_request(host,path,api_key,lQuery,{},processResponse);
				}
			}
		}
		else {
			console.log('Could not nitro.ping '+host);
		}
	});
}

//----------------------------------------------------------------------------

initDatabase(run);

process.on('exit', function(code) {
	readRows(gdb);
	gdb.close();
});