from confluent_kafka import Consumer, KafkaError
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler
import json

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'diabet-consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

topic = 'diabet'
consumer.subscribe([topic])

historical_data = pd.read_csv('Diabets.csv')

X = historical_data[['Pregnancies', 'Glucose', 'BloodPressure','SkinThickness','Insulin','BMI','DiabetesPedigreeFunction','Age']]
y = historical_data['Outcome']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), ['Pregnancies', 'Glucose', 'BloodPressure','SkinThickness','Insulin','BMI','DiabetesPedigreeFunction','Age'])
    ]
)

model = LinearRegression()

pipeline = Pipeline([
    ('preprocessor', preprocessor),
    ('model', model)
])

pipeline.fit(X_train, y_train)

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('Reached end of partition')
            else:
                print(f'Error: {msg.error()}')
        else:
            new_data = json.loads(msg.value().decode("utf-8"))
            new_data_df = pd.DataFrame([new_data], columns=['Pregnancies', 'Glucose', 'BloodPressure','SkinThickness','Insulin','BMI','DiabetesPedigreeFunction','Age'])

            predicted_out = pipeline.predict(new_data_df)[0]
            actual_out = new_data['Outcome']
            
            
            print(f'Received message: {msg.value().decode("utf-8")}')
            print(f'Predicted Price: {predicted_out}, Actual Price: {actual_out}')
            
            accuracy = 100 - mean_squared_error([actual_out], [predicted_out], squared=False)
            print(f'Accuracy: {accuracy}%')

except KeyboardInterrupt:
    pass

finally:
    consumer.close()