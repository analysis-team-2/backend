import json
import sys
import pandas as pd
from autogluon.tabular import TabularDataset, TabularPredictor
import os

def main():
    if len(sys.argv) < 2:
        print(json.dumps({"status": "error", "message": "No input data provided"}, ensure_ascii=False))
        return

    input_data = sys.argv[1]

    try:
        data = json.loads(input_data)

        # JSON 데이터를 DataFrame으로 변환
        df_predict = pd.DataFrame(data)
        df_predict = df_predict.rename(columns={
            'store_avg_period': '운영점포평균영업기간',
            'closure_avg_period': '폐업점포평균영업기간',
            'business_change_indicator': '상권변동지표구분'
        })
        df_predict = TabularDataset(df_predict)

        # 모델 로드
        model_path = r"ag-20240715_073451"
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"모델 경로를 찾을 수 없습니다: {model_path}")
        predictor2 = TabularPredictor.load(model_path)

        # 'amt' 열을 제외하고 예측
        y_pred = predictor2.predict(df_predict.drop(columns=['amt']))

        # 예측 값을 float으로 변환
        predicted_value = float(y_pred.iloc[0])

        # 결과를 JSON 형태로 반환
        result = {
            "status": "success",
            "actual_value": 365433405,  # 실제값 (예시로 입력)
            "predicted_value": predicted_value  # numpy.float32 -> float 변환
        }
    except Exception as e:
        result = {
            "status": "error",
            "message": str(e)
        }

    # 결과를 JSON 형태로 표준 출력
    print(json.dumps(result, ensure_ascii=False))

if __name__ == "__main__":
    main()
