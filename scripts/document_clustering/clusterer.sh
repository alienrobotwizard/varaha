#!/usr/bin/env bash

work_dir=$1     ; shift

if [ "$work_dir" == '' ] ; then echo "Please specify the directory containing the K initial centers and tfidf vectors: $0 work_dir [number_of_iterations] [start_iteration]" ; exit ; fi

# How many rounds to run: default 10
n_iters=${1-10} ; shift
# the iteration to start with: default 0
start_i=${1-0}  ; shift
# this directory
script_dir=$(readlink -f `dirname $0`)
tfidf=$work_dir/tfidf-vectors

for (( iter=0 ; "$iter" < "$n_iters" ; iter++ )) ; do
  curr_str=$(( $start_i + $iter ))
  next_str=$(( $start_i + $iter + 1 ))
  curr_iter_file=$work_dir/k_centers-${curr_str}
  next_iter_file=$work_dir/k_centers-${next_str}
  echo -e "\n****************************\n"
  echo -e "Iteration $(( $iter + 1 )) / $n_iters:\t `basename $curr_iter_file` => `basename $next_iter_file`"
  echo -e "\n****************************"
  pig -p TFIDF=$tfidf -p CURR_CENTERS=$curr_iter_file -p NEXT_CENTERS=$next_iter_file $script_dir/cluster_documents.pig
done
