# Article Index Data Pipelines Deployment

> Goal: Immutable, idempotent deployment.

* create `hosts.prod` file with your remote ssh username (XXX)

```ini
[index]

172.2.5.11 ansible_ssh_user=XXX ansible_ssh_args="-o ForwardAgent=yes"
```

* symlink your preferred inventory to `hosts` for `Makefile` to pick it up, e.g.

```shell
$ ln -s hosts.prod hosts
```

* deploy

```shell
$ make deploy
```

## Things left out of ansible (for now)

* changes to "/etc/hosts" that are needed for manual tasks

## TODO

* [ ] up to regular generation of indexable files
    * [ ] add cron for AIUpdate
    * [ ] check that crossref feed is stable
* [x] host w/o sudo
* [x] install common packages
