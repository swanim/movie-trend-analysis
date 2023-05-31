# 1. Create a SageMaker endpoint
# 2. Give permissions to EC2 instance to access SageMaker endpoint
# 3. Run the following code on private EC2 instance

import pandas as pd
import boto3 # 프라이빗 인스턴스에서 필요함
import io

df = pd.read_csv('pred_set.csv')

# Instantiate SageMaker runtime client
sagemaker_runtime = boto3.client('sagemaker-runtime', region_name='ap-northeast-2')

# Iterate over the rows and make predictions using SageMaker endpoint
predicted_sales = []
curr = 1
for _, row in df.iterrows():
    # Prepare the input data for prediction, same ordering as the training data
    input_data = row[['date', 'movieCd', 'genres', 'ticketRatio', 'openDt', 'showingDays']].values.tolist()  # Adjust based on your input features

    # Convert the input data to CSV format
    input_csv = io.StringIO()
    pd.DataFrame([input_data]).to_csv(input_csv, header=False, index=False)
    input_csv.seek(0)
    input_content = input_csv.getvalue()

    # Make a prediction using the SageMaker endpoint
    response = sagemaker_runtime.invoke_endpoint(
        EndpointName='movie-autoML',
        Body=input_content,
        ContentType='text/csv'
    )

    # Decode JSON
    response_body = response['Body'].read().decode()
    predicted_sales_value = round(float(response_body.strip()),2)

    print("making predictions... " + str(curr) + "/" + str(len(df)))

    predicted_sales.append(predicted_sales_value)
    curr += 1

# Add the predicted_sales column to the DataFrame
df['predicted_sales'] = predicted_sales
df.to_csv('prediction_result.csv')