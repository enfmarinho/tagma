MAIN     := main
SRCDIR   := src
BUILDDIR := build
PDF      := $(BUILDDIR)/$(MAIN).pdf

.PHONY: all pdf watch open clean

all: pdf

pdf:
	@mkdir -p $(BUILDDIR)
	latexmk -cd $(SRCDIR)/$(MAIN).tex

watch:
	@mkdir -p $(BUILDDIR)
	latexmk -pvc -cd $(SRCDIR)/$(MAIN).tex

open: pdf
	xdg-open $(PDF) 2>/dev/null || open $(PDF)

clean:
	latexmk -C -cd $(SRCDIR)/$(MAIN).tex
	rm -rf $(BUILDDIR)
