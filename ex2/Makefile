all: client

client: bw_template.c
	 gcc bw_template.c -libverbs -o server --verbose && ln -s server client

clean:
	rm -f client server

