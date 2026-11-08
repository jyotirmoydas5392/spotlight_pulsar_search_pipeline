#! /bin/csh -f 

# Set MATH alias
#     alias MATH 'set \!:1 = `echo "\!:3-$" | bc `'
#         alias MATH1 'set \!:1 = `echo "\!:3-$" | bc -l`'
#
# Script to make ps/png files for GHRSS (GMRT) candidates 
         set filename=cand_file
         set numfile=`wc -w $filename | awk '{print $1}'`
         set outdir=`/home/bhaswati/Machine_Learning/11aug2k15/`
cp cand_file /home/bhaswati/Machine_Learning/11aug2k15/plot_files 
cd /home/bhaswati/Machine_Learning/11aug2k15/plot_files
@ i = 1
while ($i <= $numfile)
set scan=`cat cand_file | head -$i | tail -1 | awk -F "HR" '{print $2}'`
set file=`cat cand_file | head -$i | tail -1`
show_pfd $file -noxwin
    @ i++
end
