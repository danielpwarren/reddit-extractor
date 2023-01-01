# This will require a large amount of storage (>6TiB)

Start scraping from 2008, this is when subreddits were made.

## download.py
multithreaded download script for the reddit data, be sure to set threads to a reasonable number!

I'm not sure why but the verification fails sometimes when the files aren't corrupt.

Just run the script again or use the command `sha256sum -c sha256sums.txt`
in the directory you stored the data

## extract.py
Extracts conversations to data/out/extract

## conv.py
Filters conversations

## concat.py
compiles all files into one