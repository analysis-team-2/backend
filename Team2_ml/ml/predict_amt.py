import json
import sys
import pandas as pd
from autogluon.tabular import TabularDataset, TabularPredictor
import os
import logging
import warnings

def main():
    warnings.filterwarnings("ignore")

    logging.getLogger("autogluon").setLevel(logging.CRITICAL)
    logging.getLogger("autogluon.core").setLevel(logging.CRITICAL)
    logging.getLogger("autogluon.tabular").setLevel(logging.CRITICAL)

    if len(sys.argv) < 2:
        print(json.dumps({"status": "error", "message": "No input data provided"}, ensure_ascii=False))
        return

    input_data = sys.argv[1]

    try:
        # JSON 데이터를 직접 디코딩
        data = json.loads(input_data)

#         print(data)  # 디버깅 메시지

        df_predict = pd.DataFrame(data)
        df_predict = df_predict.rename(columns={
            'store_avg_period': '운영점포평균영업기간',
            'shutdown_avg_period': '폐업점포평균영업기간',
            'changing_tag': '상권변동지표구분'
        })
        df_predict = TabularDataset(df_predict)

        model_path = r"ml/ag-20240715_073451"
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"모델 경로를 찾을 수 없습니다: {model_path}")

        predictor2 = TabularPredictor.load(model_path)

        y_pred = predictor2.predict(df_predict.drop(columns=['amt']))

        predicted_value = float(y_pred.iloc[0])

        result = {
            "status": "success",
            "actual_value": 365433405,
            "predicted_value": predicted_value
        }
        print(json.dumps(result, ensure_ascii=False))  # 디버깅 메시지

    except Exception as e:
        result = {
            "status": "error",
            "message": str(e)
        }
        print(json.dumps(result, ensure_ascii=False))  # 디버깅 메시지

if __name__ == "__main__":
    main()
