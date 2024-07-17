import pandas as pd
from autogluon.timeseries import TimeSeriesPredictor, TimeSeriesDataFrame
import argparse
import warnings
import logging
import json
import sys
from datetime import datetime


def load_and_predict(model_name, item_id):
    try:
        # 모델 경로 설정
        model_paths = {
            '광명시': 'ml/models/merged_data_gm',
            '시흥시': 'ml/models/merged_data_sh',
            '수원시': 'ml/models/merged_data_sw',
            '안양시': 'ml/models/merged_data_yy',
            '화성시': 'ml/models/merged_data_hs'
        }

        if model_name not in model_paths:
            raise ValueError("Invalid model name provided.")

        model_path = model_paths[model_name]

        # 모델 로드 (버전 불일치 무시)
        predictor = TimeSeriesPredictor.load(model_path, require_version_match=False)

        # 데이터 로드 및 변환
        file_path = "ml/models/train_data_time.csv"
        df = pd.read_csv(file_path)

        df = TimeSeriesDataFrame.from_data_frame(
            df,
            id_column="item_id",
            timestamp_column="timestamp"
        )

        # 예측 수행
        predictions = predictor.predict(df)

        # 2024-07-01부터 끝까지의 결과 필터링
        filtered_predictions = predictions.loc[item_id].reset_index()
        filtered_predictions = filtered_predictions[filtered_predictions['timestamp'] >= '2024-07-01']
        filtered_predictions['timestamp'] = filtered_predictions['timestamp'].apply(lambda x: x.strftime('%Y-%m-%d'))

        filtered_predictions = filtered_predictions[['timestamp', 'mean']]

        # 예측 결과 출력
        print(filtered_predictions.to_json(orient="records"))

    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

def main():
    warnings.filterwarnings("ignore")

    logging.getLogger("autogluon").setLevel(logging.CRITICAL)
    logging.getLogger("autogluon.core").setLevel(logging.CRITICAL)
    logging.getLogger("autogluon.timeseries").setLevel(logging.CRITICAL)

    parser = argparse.ArgumentParser(description="Time series prediction script.")
    parser.add_argument("model_name", type=str, help="The name of the model to use.")
    parser.add_argument("item_id", type=str, help="The item ID to predict.")

    args = parser.parse_args()

    # 예측 수행
    load_and_predict(args.model_name, args.item_id)

if __name__ == "__main__":
    main()
