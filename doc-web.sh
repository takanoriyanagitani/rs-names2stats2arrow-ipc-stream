#!/bin/sh

port=11098
addr=127.0.0.1
docd=./

miniserve \
	--port ${port} \
	--interfaces "${addr}" \
	"${docd}"
