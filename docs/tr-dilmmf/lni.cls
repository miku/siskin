% LaTeX Dokumentenklasse für die LNI Reihe der GI
%
% (C) 2002 Robert Tolksdorf
% Im Fall von Fragen zu dieser Dokumentenklasse wenden Sie sich bitte
% an mail@robert-tolksdorf.de
%
%
\ProvidesClass{lni}[2004/13/07 v0.41, by Robert Tolksdorf, Berlin]

% \newif\ifpdf\ifx\pdfoutput\undefined\pdffalse\else\pdfoutput=1\pdftrue\fi

% Satzspiegelposition gemäß LNI-Herausgeberrichtlinien (Korrektur von Thomas.Kuehne@mcs.vuw.ac.nz)
\setlength{\hoffset}{-0,2cm}% correct left margin from 4.4cm to 4.2cm
\setlength{\voffset}{0.8cm}% correct top margin from 4.53cm to 5.33cm

% Vielen Dank an Matthias Rust (mrust@rostock.zgdv.de) fuer folgende
% Ausrichtung an LNI Satzspiegel
\DeclareOption{forInclusion}{%
  \setlength{\topmargin}{0mm}%
  \setlength{\headheight}{0mm}%
  \setlength{\headsep}{0mm}%
  \setlength{\hoffset}{-10,9mm}%
  \setlength{\voffset}{-3,9mm}%
  \setlength{\oddsidemargin}{0mm}%
  \setlength{\evensidemargin}{0mm}%
  \setlength{\paperheight}{235mm}%
  \setlength{\paperwidth}{155mm}
  \ifpdf%
    \setlength{\pdfpageheight}{235mm}%
    \setlength{\pdfpagewidth}{155mm}%
  \fi}

\newif\iflnienglish\lnienglishfalse
\DeclareOption{english}{\lnienglishtrue}

\ProcessOptions

% 10 pt
\LoadClass[10pt,a4paper,fleqn]{article}
\RequirePackage[english,ngerman]{babel}
\iflnienglish\selectlanguage{english}\def\bbl@main@language{english}\fi
% Blocksatz durch LaTeX-Default
% Satzspiegel
\setlength{\textheight}{192mm}
\setlength{\textwidth}{126mm}

% Times als Grundschrift
\RequirePackage{times}
% Hyphenation
% Durch normale LaTeX-Parameter verhindert (\doublehyphendemerits)

% Title: Kopie aus article.cls mit anderem \thispagestyle
\renewcommand\maketitle{\par
  \begingroup
    \renewcommand\thefootnote{\@fnsymbol\c@footnote}%
    \def\@makefnmark{\rlap{\@textsuperscript{\normalfont\@thefnmark}}}%
    \long\def\@makefntext##1{\parindent 1em\noindent
            \hb@xt@1.8em{%
                \hss\@textsuperscript{\normalfont\@thefnmark}}##1}%
    \if@twocolumn
      \ifnum \col@number=\@ne
        \@maketitle
      \else
        \twocolumn[\@maketitle]%
      \fi
    \else
      \newpage
      \global\@topnum\z@   % Prevents figures from going at top of page.
      \@maketitle
    \fi
    \thispagestyle{empty}\@thanks
  \endgroup
  \setcounter{footnote}{0}%
  \global\let\thanks\relax
  \global\let\maketitle\relax
  \global\let\@maketitle\relax
  \global\let\@thanks\@empty
  \global\let\@author\@empty
  \global\let\@date\@empty
  \global\let\@title\@empty
  \global\let\title\relax
  \global\let\author\relax
  \global\let\date\relax
  \global\let\and\relax
}
\def\@maketitle{%
  \newpage
  \null
  \vskip 2em%
  \begin{center}%
  \let \footnote \thanks
    {\fontsize{14}{17}\fontseries{b}\selectfont \@title \par}%
    \vskip 1.5em%
    {\fontsize{10}{12}\selectfont
      \lineskip .5em%
      \begin{tabular}[t]{c}%
        \@author
      \end{tabular}\par}%
    \vskip 1\baselineskip%
  \end{center}%
  \par
  }

% Abstract
\renewenvironment*{abstract}{%
 \renewcommand{\abstractname}{Abstract}
 \fontsize{9}{10}\selectfont
    \quotation\noindent{\bfseries \abstractname:}%
}{\endquotation}

% Überschriften
\renewcommand{\section}{\@startsection{section}{1}{\z@}%
  {2\baselineskip}{\baselineskip}{\fontsize{12}{14}\fontseries{b}\selectfont}}
\renewcommand{\subsection}{\@startsection{subsection}{2}{\z@}%
  {2\baselineskip}{\baselineskip}{\fontsize{10}{12}\fontseries{b}\selectfont}}
% weitere nicht durch Formatvorlage geklärt

% keine seitennummern
\pagestyle{empty}

% Bildunterschriften
\long\def\@makecaption#1#2{%
  \vskip\abovecaptionskip
  {\fontsize{9}{10}\selectfont
   \sbox\@tempboxa{#1: #2}%
  \ifdim \wd\@tempboxa >\hsize
    #1: #2\par
  \else
    \global \@minipagefalse
    \hb@xt@\hsize{\hfil\box\@tempboxa\hfil}%
  \fi}
  \vskip\belowcaptionskip}

% gleichungen mit richtiger einrückung
% fleqn-Option oben
\setlength{\mathindent}{4ex}

% listings mit richtiger einrückung
\RequirePackage{verbatim}
\def\verbatim@processline{\hskip4ex\the\verbatim@line\par}

% Fussnoten mit 5cm Linie und richtiger Größe
\renewcommand\footnoterule{%
  \kern-3\p@
  \hrule\@width5cm
  \kern2.6\p@}
\renewcommand\@makefntext[1]{%
    \parindent 1em%
    \noindent
    \hb@xt@1.8em{\hss\@makefnmark}{\fontsize{8}{9}\selectfont#1}}

% Gemutmaßte Absatzeinzüge und Abstände
\parindent0pt
\parskip 1ex

% bibliography
\iflanguage{english}{\bibliographystyle{lni}}{\bibliographystyle{lnig}}
\renewenvironment{thebibliography}[1]
     {%\def\refname{Literaturverzeichnis}
      \section*{\refname}%
      \bgroup\fontsize{9}{10}\selectfont
      \list{\@biblabel{\@arabic\c@enumiv}}%
           {\settowidth\labelwidth{\@biblabel{#1}}%
            \leftmargin\labelwidth
            \advance\leftmargin\labelsep
            \@openbib@code
            \usecounter{enumiv}%
            \let\p@enumiv\@empty
            \renewcommand\theenumiv{\@arabic\c@enumiv}}%
      \sloppy
      \clubpenalty4000
      \@clubpenalty \clubpenalty
      \widowpenalty4000%
      \sfcode`\.\@m}
     {\def\@noitemerr
       {\@latex@warning{Empty `thebibliography' environment}}%
      \endlist\egroup}
