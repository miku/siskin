SHELL := /bin/bash

.PHONY: all
all:
	# This generates JSON data.
	git-of-theseus-analyze ../..
	# Create charts.
	git-of-theseus-stack-plot authors.json && mv stack_plot.png authors.png
	git-of-theseus-stack-plot cohorts.json && mv stack_plot.png cohorts.png
	git-of-theseus-stack-plot exts.json && mv stack_plot.png exts.png
	git-of-theseus-survival-plot survival.json && mv survival_plot.png survival.png

