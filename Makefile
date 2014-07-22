help:
	@echo "make clean"

clean:
	rm -rf siskin.egg-info
	rm -rf build/ dist/

# packaging via vagrant
SSHCMD = ssh -o StrictHostKeyChecking=no -i vagrant.key vagrant@127.0.0.1 -p 2222

# Helper to build RPM on a RHEL6 VM, to link against glibc 2.12
vagrant.key:
	curl -sL "https://raw2.github.com/mitchellh/vagrant/mastekeys/vagrant" > vagrant.key
	chmod 0600 vagrant.key

vm-setup: vagrant.key
	$(SSHCMD) git clone https://github.com/miku/siskin.git

# vm-rpm:
# 	$(SSHCMD) "cd siskin && git pull origin master && cat requirements.txt | while read line; do fpm -s python -t rpm $line; done"
