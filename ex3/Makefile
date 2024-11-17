all: client

client: bw_template.c
	 gcc bw_template.c -libverbs -o node --verbose

clean:
	rm -f node

