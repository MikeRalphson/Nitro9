# Nitro9

A tool to index available programmes on the BBC iPlayer / radio-player for offline analysis.

This is a scraper that runs on [Morph](https://morph.io). Please read [the Morph.io documentation](https://morph.io/documentation).

It can also be run locally.

Set an environment variable called `MORPH_API_KEY` to your Nitro API key. When initially run on an empty / non-existant database, a
full scrape of available programmes is performed. Subsequently, only those programmes broadcast since the last update will be added.
Programmes passed their expiry date are removed.

To build a full index again, either delete the database or set the environment variable `MORPH_REBUILD` to `true`.

Usage: `node scraper.js`

Output is an sqlite3 database containing one table, "data":

````javascript
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
````

Also included is a simple command line utility called `query.js` for performing SQL queries against the remote Morph.io database and
returning the rows in JSON format.

Usage: `node query {SQL-statement}`