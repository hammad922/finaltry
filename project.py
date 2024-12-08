from flask import Flask, request, jsonify
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from datetime import datetime, timedelta


# InfluxDB Cloud setup
INFLUXDB_URL = "https://eu-central-1-1.aws.cloud2.influxdata.com/"
INFLUXDB_TOKEN = "UULJ94Wz_3V5s423E3dGCgYm5lbknZGC9eIE_cScWNOt2nA8suUeEFHdg_8tYdt74poQMIDIwL4XF2ke-__MRw=="
INFLUXDB_ORG = "org"
INFLUXDB_BUCKET = "bucket"

client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = client.query_api()
write_api = client.write_api(write_options=SYNCHRONOUS)

app = Flask(__name__)

# MQTT setup
mqtt_broker = "test.mosquitto.org"
mqtt_port = 1883
mqtt_client = mqtt.Client()

# MQTT on_connect callback
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("plant/#")

# MQTT on_message callback
def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload.decode()))

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

mqtt_client.connect(mqtt_broker, mqtt_port, 60)
mqtt_client.loop_start()

samplingRateTopicLDR1 = "plant/ldr1/samplingRate"
samplingRateTopicLDR2 = "plant/ldr2/samplingRate"


# MQTT publish function
def publish_sampling_data(sampling_rate_ldr1, sampling_rate_ldr2):
    try:
        mqtt_client.publish(samplingRateTopicLDR1, str(sampling_rate_ldr1))
        mqtt_client.publish(samplingRateTopicLDR2, str(sampling_rate_ldr2))
        print(f"Published sampling rates: LDR1 - {sampling_rate_ldr1} seconds, LDR2 - {sampling_rate_ldr2} seconds")
    except Exception as e:
        print(f"Failed to publish sampling data: {e}")


@app.route('/update-sampling-rate', methods=['POST'])
def update_sampling_rate():
    try:
        data = request.get_json()
        sampling_rate_ldr1 = data.get('samplingRateLDR1', 10)  
        sampling_rate_ldr2 = data.get('samplingRateLDR2', 10) 
        
        # Publish the new sampling rate values to MQTT
        publish_sampling_data(sampling_rate_ldr1, sampling_rate_ldr2)

        return jsonify({
            "success": True, 
            "message": "Sampling rate updated and published to MQTT",
            "samplingRateLDR1": sampling_rate_ldr1,
            "samplingRateLDR2": sampling_rate_ldr2
        }), 200

    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 400



@app.route('/post-data', methods=['POST'])
def post_data():
    try:
        # Get data from the request
        ldr1 = request.form.get('ldr1')
        ldr2 = request.form.get('ldr2')
        position = request.form.get('position')

        # Ensure data is valid
        if ldr1 is None or ldr2 is None or position is None:
            raise ValueError("Missing sensor data or position")

        # Create a Point for InfluxDB
        point = Point("light_data") \
            .tag("position", position) \
            .field("ldr1", int(ldr1)) \
            .field("ldr2", int(ldr2)) \
            .time(datetime.utcnow(), WritePrecision.NS)

        # Write data to InfluxDB Cloud
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

        # Publish data to MQTT broker
        mqtt_client.publish("plant/ldr1", ldr1)
        mqtt_client.publish("plant/ldr2", ldr2)
        mqtt_client.publish("plant/position", position)

        return jsonify({"success": True, "message": "Data written to InfluxDB and published to MQTT"}), 200
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 400

def fetch_data(position, hours=24):
    query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: -{hours}h)
      |> filter(fn: (r) => r._measurement == "light_data")
      |> filter(fn: (r) => r.position == "{position}")
      |> aggregateWindow(every: 1h, fn: mean)
      |> sort(columns: ["_time"], desc: false)
    '''
    result = query_api.query(org=INFLUXDB_ORG, query=query)
    records = result[0].records if result else []
    data = [{**record.values, '_time': record.get_time()} for record in records]
    df = pd.DataFrame(data)
    if not df.empty:
        df = df.pivot(index='_time', columns='_field', values='_value').reset_index()
    return df

def predict_solar_light(df, hours=24):
    df['_time'] = pd.to_datetime(df['_time'])
    df.set_index('_time', inplace=True)
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=['ldr1'])  # Drop NaN values only for ldr1
    
    
    X = np.array(range(len(df))).reshape(-1, 1)
    y = df['ldr1'].values

    model = LinearRegression()
    model.fit(X, y)
    
    # Predict future values for ldr1
    future_X = np.array(range(len(df), len(df) + hours)).reshape(-1, 1)
    prediction_ldr1 = model.predict(future_X)
    
    # Generate future time intervals
    future_dates = [df.index[-1] + timedelta(hours=i+1) for i in range(hours)]
    
    # Create DataFrame for predicted ldr1 values
    prediction_df = pd.DataFrame({
        '_time': future_dates,
        'prediction_ldr1': prediction_ldr1
    })
    
    return prediction_df



def write_predictions(predictions, position):
    points = []
    print(f"Predictions to be written: {predictions}")  # Debug statement
    for index, row in predictions.iterrows():
        point = Point("light_prediction") \
            .tag("position", position) \
            .field("predicted_ldr1", row['prediction_ldr1']) \
            .time(row['_time'], WritePrecision.NS)
        points.append(point)
    
    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)



def determine_optimal_position():
    positions = ["living_room", "living_room2", "balcony", "bedroom_window"]
    position_light = {}
    
    for position in positions:
        data = fetch_data(position, hours=24)
        if data.empty:
            continue
        prediction_df = predict_solar_light(data, hours=24)
        total_light_ldr1 = prediction_df['prediction_ldr1'].sum()
        total_light_ldr2 = prediction_df['prediction_ldr2'].sum()
        total_light = total_light_ldr1 + total_light_ldr2
        position_light[position] = total_light
    
    if not position_light:
        raise ValueError("No data available for any positions")
    
    optimal_position = max(position_light, key=position_light.get)
    return optimal_position, position_light

@app.route('/run-analytics', methods=['POST'])
def run_analytics():
    try:
        data = request.get_json()
        print(f"Received data: {data}")  # Debug statement
        position = data.get("position")
        if not position:
            return jsonify({"success": False, "message": "Position not provided"}), 400
        
        data = fetch_data(position, hours=24)
        print(f"Fetched data: {data.head()}")  # Debug statement
        if data.empty:
            return jsonify({"success": False, "message": "No data found for the given position"}), 400
        prediction_df = predict_solar_light(data, hours=24)
        print(f"Prediction: {prediction_df.head()}")  # Debug statement
        write_predictions(prediction_df, position)
        
        
        optimal_position, position_light = determine_optimal_position()
        print(f"Optimal position: {optimal_position}, Light data: {position_light}")  # Debug statement
        return jsonify({"success": True, "message": "Data analytics completed successfully", "optimal_position": optimal_position, "position_light": position_light}), 200
    except Exception as e:
        print(f"Error: {e}")  # Debug statement
        return jsonify({"success": False, "message": str(e)}), 400


@app.route('/get-predictions', methods=['GET'])
def get_predictions():
    position = request.args.get('position')
    if not position:
        return jsonify({"success": False, "message": "Position not provided"}), 400

    query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: -24h)
      |> filter(fn: (r) => r._measurement == "light_prediction")
      |> filter(fn: (r) => r.position == "{position}")
      |> keep(columns: ["_time", "predicted_ldr1"])
      |> sort(columns: ["_time"], desc: false)
    '''
    result = query_api.query(org=INFLUXDB_ORG, query=query)
    records = result[0].records if result else []
    data = [{**record.values, '_time': record.get_time()} for record in records]
    
    return jsonify({"success": True, "data": data}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)