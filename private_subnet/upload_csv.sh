#!/usr/bin/bash

BUCKET_NAME="dev-course-project-2-bucket"

for x in sale codes summary genre director company grade
do
    FILE_PATH="/home/ec2-user/project/raw_data/${x}/movie_${x}.csv"

    DEST_S3_PATH="s3://${BUCKET_NAME}/raw_data/daily/"

    aws s3 cp $FILE_PATH $DEST_S3_PATH

    if [ $? -eq 0 ]; then
        rm $FILE_PATH
        echo "Upload of $FILE_PATH is successful"

    else
        echo "Upload of $FILE_PATH is failed"

    fi
done

aws s3 cp "/home/ec2-user/project/prediction_result.csv" "s3://dev-course-project-2-bucket/raw_data/daily/"

rm "/home/ec2-user/project/prediction_result.csv"

rm "/home/ec2-user/project/pred_set.csv"

