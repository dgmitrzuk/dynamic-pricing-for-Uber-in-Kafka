import numpy as np
import pandas as pd
import xgboost as xg
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error as MSE

url = (
    "https://raw.githubusercontent.com/dgmitrzuk/dynamic-pricing-Uber-Kafka/"
    "refs/heads/main/dynamic_pricing.csv"
)
df = pd.read_csv(url)

y = df["Historical_Cost_of_Ride"]
X = df.drop(columns=["Historical_Cost_of_Ride"])


X = pd.get_dummies(X, drop_first=True)

train_X, test_X, train_y, test_y = train_test_split(
    X, y, test_size=0.30, random_state=123
)



xgb_r = xg.XGBRegressor(
    objective="reg:squarederror",
    n_estimators=300,       # feel free to tune
    learning_rate=0.1,
    max_depth=6,
    subsample=0.8,
    colsample_bytree=0.8,
    seed=123,
    enable_categorical=True
)


xgb_r.fit(train_X, train_y)

FEATURE_COLUMNS = list(train_X.columns)

def predict_price(df):
    df = df.reindex(columns=FEATURE_COLUMNS, fill_value=0)
    price = xgb_r.predict(df)
    return price