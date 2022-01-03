import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.graphics.tsaplots import plot_acf,plot_pacf 
from statsmodels.tsa.seasonal import seasonal_decompose 
from pmdarima import auto_arima                        
from sklearn.metrics import mean_squared_error
from statsmodels.tools.eval_measures import rmse
import warnings
warnings.filterwarnings("ignore")
import boto3
import base64
from botocore.exceptions import ClientError
import json
import os
import sys
from io import StringIO 
import matplotlib.pyplot as plt


class ArimaImplementation:
        
    def Datapreprocessing(self,data):
            data.date = pd.to_datetime(data.date)
            data1 = data.set_index("date")
            return data1
        
    def Model_train_booking(self,data1):
#             print(data1)
            train_data = data1[:len(data1)-50]
            test_data = data1[len(data1)-50:]
#             print(test_data,test_data.columns)
            arima_model = SARIMAX(train_data['number_of_booking'], order = (2,1,1), seasonal_order = (1,1,1,12))
            arima_result = arima_model.fit()
            arima_pred = arima_result.predict(start = len(train_data), end = len(data1)-1, typ="levels").rename("number_of_booking")
#             print(arima_pred)
#             data = data.drop(['date'],axis=1)
#             data = data.loc[data.year == data['year'].max()]
#             data['category_description'] = 'FORECAST'
#             data['data_source_description'] = 'ARIMA'
#             data['created_by'] = username
#             data['created_at'] = pd.Timestamp.now()

            test_data = test_data.reset_index()
            test_data = test_data.drop(['number_of_booking'], axis=1)
            test_data = pd.concat([test_data, pd.DataFrame(arima_pred, columns=['number_of_booking']).reset_index(drop=True)], axis=1)
#            test_data['number_of_booking'] = arima_pred
            test_data['model_name'] = 'ARIMA'
            test_data['data_source'] = 'MODEL'
            test_data['data_category'] = 'FORECAST'
            #Add date,booking,model_accuracy,model_prob
            print("len(test_data)",len(test_data))
            print(test_data)
            return test_data

        
    def Model_train_revenue(self,data1):
#             print(data1)
            train_data = data1[:len(data1)-50]
            test_data = data1[len(data1)-50:]
            print(test_data.dtypes)
            print(train_data.dtypes)
            print(test_data,train_data)
            arima_model = SARIMAX(train_data['revenue'].astype(float), order = (2,1,1), seasonal_order = (1,1,1,12))
            arima_result = arima_model.fit()
            arima_pred = arima_result.predict(start = len(train_data), end = len(data1)-1, typ="levels").rename("revenue")
#             print(arima_pred)
#             data = data.drop(['date'],axis=1)
#             data = data.loc[data.year == data['year'].max()]
#             data['category_description'] = 'FORECAST'
#             data['data_source_description'] = 'ARIMA'
#             data['created_by'] = username
#             data['created_at'] = pd.Timestamp.now()

            test_data = test_data.reset_index()
            test_data = test_data.drop(['revenue'], axis=1)
            test_data = pd.concat([test_data, pd.DataFrame(arima_pred, columns=['revenue']).reset_index(drop=True)], axis=1)
#            test_data['number_of_booking'] = arima_pred
            test_data['model_name'] = 'ARIMA'
            test_data['data_source'] = 'MODEL'
            test_data['data_category'] = 'FORECAST'
            #Add date,booking,model_accuracy,model_prob
            test_data['revenue'] = test_data['revenue'].astype(float).astype(int)
            print("len(test_data)",len(test_data))
            print(test_data)
            return test_data
        
        
    def AccuracyBooking(self,test_data,arima_pred): 
            arima_rmse_error = rmse(test_data['booking'], arima_pred)
            arima_mse_error = arima_rmse_error**2

            print(f'MSE Error: {arima_mse_error}\nRMSE Error: {arima_rmse_error}\nMean: {mean_value}')
            test_data['ARIMA_Predictions'] = arima_pred

            errors = pd.DataFrame({"Models" : ["ARIMA"],"RMSE Errors" : arima_rmse_error, "MSE Errors" : arima_mse_error})
            print(errors)
