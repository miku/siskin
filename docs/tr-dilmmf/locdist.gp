set boxwidth 0.8
unset colorbox
set title "LOC distribution over 317 task"
set xlabel "LOC (business logic) per task"
set ylabel "Number of tasks"
set style fill solid
set terminal png size 800,600 enhanced font "Helvetica,14"
set output 'locdist.png'
plot "locdist.dat" with boxes notitle
