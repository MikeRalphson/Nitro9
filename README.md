# Nitro9

A tool to index available programmes on the BBC iPlayer / radio-player for offline analysis.

This is a scraper that runs on [Morph](https://morph.io). Please read [the Morph.io documentation](https://morph.io/documentation).

It can also be run locally.

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
