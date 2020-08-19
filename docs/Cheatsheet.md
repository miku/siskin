# Cheatsheet

> Long running scripts ahead; please run in screen or tmux (or anything else).

The current version?

```
$ taskversion
0.102.2
```

Where are all files located?

```
$ taskhome
/media/titan/siskin-data
```

## Create Files

Generate `AIExport` (solr) and `AIRedact` (blob) artifacts. This may take many
(6-24) hours, depending on the date and already finished tasks. One large
source, CrossRef, is updated monthly - and it takes a few hours to fetch the
new records from their API.

```
$ taskdo AIUpdate --workers 8 --AILicensing-override
```

While running, the progress can be inspected with `taskps` (in watch):

```
$ watch taskps
```

Once done, you can (hopefully) inspect task outputs:

```
$ taskls AIExport
$ taskls AIRedact
```

## Live Servers

Show live and nonlive solr servers.

```
$ curl -s ai.ub.uni-leipzig.de/whatislive

upstream microblob_live         { server 172.18.113.98:8820; }
upstream microblob_nonlive      { server 172.18.113.99:8820; }
upstream solr_live              { server 172.18.113.7:8085; }
upstream solr_nonlive           { server 172.18.113.15:8085; }
```

## SOLR

Delete the old data from index and reindex (nonlive server needs to be set manually; takes about 10h):

```
$ time solrbulk -purge -purge-pause 20s -w 20 -verbose -z -server 172.18.113.15:8085/solr/biblio $(taskoutput AIExport)
```

## Blob

Copy blobfile to nonlive (e.g. 172.18.113.99) blobserver (takes about 20min).

```
$ scp $(taskoutput AIRedact) 172.18.113.99:/tmp
```

Log into nonlive blobserver:

```
$ ssh -A name@172.18.113.99
$ root -A
```

Delete any old blobserver files:

```
$ cd /var/microblob
$ rm -rf date-*
```

Uncompress new file (takes about 20min), change owner.

Filename does not matter, but it used to be close to the compressed file name,
e.g. uncompress `date-2020-08-01.ldj.gz` into `date-2020-08-01.ldj` and so on.

```
$ unpigz -c /tmp/date-2020-08-01.ldj.gz > /var/microblob/date-2020-08-01.ldj
$ chown memcachedb.memcachedb /var/microblob/date-2020-08-01.ldj
```

Edit the `/usr/local/bin/startmicroblob.sh` file.

```bash
# will start a listener on 8820
# make sure blob file perms are memcachedb.memcachedb

cd /var/microblob
su memcachedb -c "microblob -log /var/log/microblob.log -addr 172.18.113.99:8820 -key finc.id date-2020-08-01.ldj"
```

Replace `date-2020-08-01.ldj` with the filename in `/var/microblob`.

Then run, in `/var/microblob`, this will first "index" the file (takes about 1h) then switch to "serve" mode.

```
$ chown -R memcachedb.memcachedb date-*.ldj* && pkill microblob; nohup startmicroblob.sh &
```

When microblob finished indexing the file, it will respond to HTTP requests:

```
$ curl -s 172.18.113.99:8820 | jq .
{
  "name": "microblob",
  "stats": "http://172.18.113.99:8820/stats",
  "vars": "http://172.18.113.99:8820/debug/vars",
  "version": "0.2.6"
}
```

The keys can be directly appended to the hostport:

```
$ curl -s 172.18.113.99:8820/ai-49-aHR0cDovL2R4LmRvaS5vcmcvMTAuMTAzNy8xMDE2Ny0wMDU | jq -rc '.url'
["http://dx.doi.org/10.1037/10167-005"]
```

## Cleanup

Occasional cleanup with
[taskgc](https://git.sc.uni-leipzig.de/ubl/finc/index/siskin/-/blob/master/bin/taskgc).
This will interactively try to free disk space.

```
$ taskgc
```

## Switching servers

Edit nginx configuration snippet (on proxy):

```
$ vim /etc/nginx/which_SOLR-which_BLOB_is_live
```

Then check and reload:

```
$ nginx -t
$ systemctl reload nginx
```

It takes a few seconds (to minutes) until SOLR is warmed up.
