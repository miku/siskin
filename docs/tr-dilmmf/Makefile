all: tr-dilmmf.pdf

tr-dilmmf.pdf: locdist.png depgraph.pdf
	pdflatex tr-dilmmf.tex && bibtex tr-dilmmf && pdflatex tr-dilmmf.tex && pdflatex tr-dilmmf.tex

depgraph.pdf:
	dot -Tpdf depgraph.dot > depgraph.pdf

locdist.png:
	gnuplot locdist.gp

clean:
	rm -f depgraph.pdf
	rm -f locdist.png
	rm -f tr-dilmmf.pdf
	rm -f *.aux
	rm -f *.bbl
	rm -f *.blg
	rm -f *.log
	rm -f *.out
