# AI Preprocessing Deployment

* create `hosts.prod` file with your remote ssh username and "-A"

```
[index]

172.2.5.11 ansible_ssh_user=XXX ansible_ssh_args="-o ForwardAgent=yes"
```

* symlink your preferred inventory to `hosts` for `Makefile` to pick it up, e.g.

```
$ ln -s hosts.prod hosts
```

* deploy

```
$ make deploy
```

# TODO

* [ ] up to regular generation of indexable files
* [x] host w/o sudo
* [x] install common packages
