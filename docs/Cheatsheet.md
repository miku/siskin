# Cheatsheet

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

Generate `AIExport` (solr) and `AIRedact` (blob) artifacts. This may take a few hours.

```
$ taskdo AIUpdate --workers 8 --AILicensing-overrideride
```

Inspect task outputs:

```
$ taskls AIExport
$ taskls AIRedact
```

Show live and nonlive solr servers.

```
$ curl -s ai.ub.uni-leipzig.de/whatislive

upstream microblob_live         { server 172.18.113.98:8820; }
upstream microblob_nonlive      { server 172.18.113.99:8820; }
upstream solr_live              { server 172.18.113.7:8085; }
upstream solr_nonlive           { server 172.18.113.15:8085; }
```

Delete the old data from index and reindex (nonlive server needs to be set manually; takes about 10h):

```
$ time solrbulk -purge -purge-pause 20s -w 20 -verbose -z -server 172.18.113.15:8085/solr/biblio $(taskoutput AIExport)
```

Copy blobfile to nonlive blobserver (takes about 20min).

```
$ scp $(taskoutput AIRedact) 172.18.113.99:/tmp
```

Log into nonlive blobserver:

```
$ ssh name@172.18.113.99
$ root
```

Delete any old blobserver files:

```
$ cd /var/microblob
$ rm -rf date-*
```

Uncompress new file (takes about 20min), change owner.

The file will have a name like: `date-2020-08-01.ldj.gz`

```
$ unpigz -c /tmp/date-2020-08-01.ldj.gz > /var/microblob/date-2020-08-01.ldj
$ chown memcachedb.memcachedb /var/microblob/date-2020-08-01.ldj
```

Edit the `/usr/local/bin/startmicroblob.sh` file.

```bash
# will start a listener on 8820
# make sure blob file perms are memcachedb.memcachedb

cd /var/microblob
su memcachedb -c "microblob -log /var/log/microblob.log -addr 172.18.113.99:8820 -key finc.id date-2020-03-30.ldj"
```

Replace `date-2020-03-30.ldj` with the filename in `/var/microblob`.

Then run, in `/var/microblob`:

```
$ chown -R memcachedb.memcachedb date-*.ldj* && pkill microblob; nohup startmicroblob.sh &
```

