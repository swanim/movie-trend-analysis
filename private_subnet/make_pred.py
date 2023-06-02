# 1. Create a SageMaker endpoint
# 2. Give permissions to EC2 instance to access SageMaker endpoint
# 3. Run the following code on private EC2 instance

import pandas as pd
import boto3
import io
import json

df = pd.read_csv('/home/ec2-user/project/pred_set.csv')

# Instantiate SageMaker runtime client
sagemaker_runtime = boto3.client('sagemaker-runtime', region_name='YOUR_REGION')

# Iterate over the rows and make predictions using SageMaker endpoint
predicted_sales = []
curr = 1
for _, row in df.iterrows():
    # Prepare the input data for prediction
    input_data = row[['date', 'movieCd', 'genres', 'ticketRatio', 'openDt', 'showingDays']].values.tolist()  # Adjust based on your input features

    # Convert the input data to CSV format
    input_csv = io.StringIO()
    pd.DataFrame([input_data]).to_csv(input_csv, header=False, index=False)
    input_csv.seek(0)
    input_content = input_csv.getvalue()

    # Make a prediction using the SageMaker endpoint
    response = sagemaker_runtime.invoke_endpoint(
        EndpointName='YOUR_ENDPOINT_NAME',
        Body=input_content,
        ContentType='text/csv'
    )

    response_body = response['Body'].read().decode()
    predicted_sales_value = round(float(response_body.strip()),2)

    print("making predictions... " + str(curr) + "/" + str(len(df)))

    predicted_sales.append(predicted_sales_value)
    curr += 1

# Add the predicted_sales column to the DataFrame
df['predicted_sales'] = predicted_sales
df.to_csv('/home/ec2-user/project/prediction_result.csv')

