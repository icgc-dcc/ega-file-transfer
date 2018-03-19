#/bin/bash

input_dir=$1
jt_server="http://10.0.0.14:8001"

jobs=`curl -X GET --header "Accept: application/json" "$jt_server/api/jt-jess/v0.1/jobs/owner/baminou/queue/30b05f9e-982e-4c66-b7ae-2028a2bffa81?state=completed"`
job_jsons=$(for bundle_id in `echo $jobs | jq -rc '.[] | .job_file' | jq -r .bundle_id`
do
    ls queued-jobs | grep $bundle_id
done)

for file in `echo "$job_jsons"`;do folder=$(echo $file | sed 's/.json//g'); mkdir $input_dir/completed-jobs/$folder; mv $input_dir/queued-jobs/$file $input_dir/completed-jobs/$folder/$file;done
