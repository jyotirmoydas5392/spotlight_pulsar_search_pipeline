#!/bin/bash


for obs in `cat /home/cooper/GHRSS-GPU-Cands/obs.lis`

do
    echo $obs
    java -jar ML.jar -v -m/home/cooper/HTRU_CLASSIFIER_STUFF/HTRU_DT.model -o/home/cooper/GHRSS-GPU-Cands/$obs/${obs}prediction.txt -p/home/cooper/GHRSS-GPU-Cands/$obs/${obs}scores.arff -a1
done
