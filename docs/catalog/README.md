Creating gifs:

    $ for i in AIExport.2015*; do gm convert "$i" "${i%.png}.gif"; done
    $ gifsicle --optimize=3 --delay=3 AIExport.*.gif > AIExport.gif
 
