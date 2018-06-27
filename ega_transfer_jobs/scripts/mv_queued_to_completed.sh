#/bin/bash

input_dir=$1
queue_id=$3
username=$2
jt_server="http://10.0.0.14:8001"

jobs=`curl -X GET --header "Accept: application/json" "$jt_server/api/jt-jess/v0.1/jobs/owner/$username/queue/$queue_id?state=completed"`
job_jsons=$(for bundle_id in `echo $jobs | jq -rc '.[] | .job_file' | jq -r .bundle_id`
do
    ls $input_dir/queued-jobs | grep $bundle_id
done)

for file in `echo "$job_jsons"`;do folder=$(echo $file | sed 's/.json//g'); mkdir $input_dir/completed-jobs/$folder; mv $input_dir/queued-jobs/$file $input_dir/completed-jobs/$folder/$file;done
