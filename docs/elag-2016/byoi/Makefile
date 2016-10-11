all: makeimages
	pandoc -t beamer -V theme:default -colortheme:default slides.md -o slides.pdf

clean:
	rm -f slides.pdf
	find . -name ".DS_Store" -exec rm -f {} \;
	find . -name "*.pyc" -exec rm -f {} \;

makeimages:
	cd images && make && cd -

Bootcamp: clean all
	rsync -avzP -e 'ssh -p 2200 -i .insecure_key' --exclude=.gitignore --exclude=.git --exclude=vm-images . byoi@localhost:/home/byoi/Bootcamp

more.pdf: more.md
	pandoc -t beamer -V theme:default -colortheme:default more.md -o more.pdf

slides-vude.pdf: slides-vude.md
	pandoc -t beamer -V theme:default -colortheme:default slides-vude.md -o slides-vude.pdf
