#!/bin/sh

echo example 1 using arrow-cat
ls . |
./names2stats2arrow-ipc-stream |
	arrow-cat |
	tail -3

echo
echo example 2 using sql
ls . |
./names2stats2arrow-ipc-stream \
	--batch-size 128 |
	rs-ipc-stream2df \
	--max-rows 1024 \
	--tabname 'file_stats' \
	--sql "
		SELECT
			*
		FROM file_stats
		ORDER BY nlink DESC
		LIMIT 3
	" |
	rs-arrow-ipc-stream-cat
