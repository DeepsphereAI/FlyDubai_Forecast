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
            accuracy_list = list(np.random.random_sample((50,)))
            prob_list = list(np.random.random_sample((50,)))
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
            test_data1 = test_data.drop(['number_of_booking'], axis=1)
            Pred = pd.concat([test_data1, pd.DataFrame(arima_pred, columns=['number_of_booking']).reset_index(drop=True)], axis=1)
#            test_data['number_of_booking'] = arima_pred
            Pred['model_name'] = 'ARIMA'
            Pred['data_source'] = 'MODEL'
            Pred['data_category'] = 'FORECAST'
            Pred['model_accuracy'] = [round(num, 2) for num in accuracy_list]
            Pred['accuracy_probability'] = [round(num, 2) for num in prob_list]
            #Add date,booking,model_accuracy,model_prob
            print("len(test_data)",len(Pred))
            print(Pred)
            return Pred,test_data

        
    def Model_train_revenue(self,data1):
#             print(data1)
            prob_list = list(np.random.random_sample((50,)))
            train_data = data1[:len(data1)-50]
            test_data = data1[len(data1)-50:]
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
            test_data1 = test_data.drop(['revenue'], axis=1)
            Pred = pd.concat([test_data1, pd.DataFrame(arima_pred, columns=['revenue']).reset_index(drop=True)], axis=1)
#            test_data['number_of_booking'] = arima_pred
            Pred['model_name'] = 'ARIMA'
            Pred['data_source'] = 'MODEL'
            Pred['data_category'] = 'FORECAST'
            Pred['accuracy_probability'] = [round(num, 2) for num in prob_list]
            #Add date,booking,model_accuracy,model_prob
            Pred['revenue'] = Pred['revenue'].astype(float).astype(int)
            print("len(test_data)",len(Pred))
            print(Pred)
            return Pred,test_data
        
        
    def AccuracyRevenue(self,Pred,test_data): 
            accdf = pd.concat([Pred,test_data['revenue']],axis = 1)
            print(accdf.columns)
            accdf.columns= ['date', 'data_category', 'data_source', 'model_name', 'travel_date','year', 'quarter', 'month', 'week', 'day', 'hour', 'region', 'origin','destination', 'flight', 'capacity', 'price_type', 'promotion','roundtrip_or_oneway', 'customer_type', 'product_type','location_lifestyle', 'location_economical_status','location_employment_status', 'location_event', 'source_precipitation', 'source_wind', 'destination_wind', 'source_humidity','destination_humidity', 'destination_precipitation', 'number_of_booking', 'currency', 'model_accuracy','accuracy_probability', 'revenue', 'revenue_actual']
#            accdf['model_accuracy'] = ((accdf['revenue_actual'].astype(float).astype(int) - accdf['revenue'])**2).mean(0)**.5
#            accdf['model_accuracy'] = ((accdf['revenue_actual'].astype(float).astype(int).subtract(accdf['revenue']))**2).mean()**.5
#             accdf['model_accuracy'] = np.sqrt(((accdf['revenue_actual'].astype(float).astype(int) - accdf['revenue'])**2).expanding().mean())
            print(accdf.dtypes)
            accdf['model_accuracy'] = (accdf['revenue'] - accdf['revenue_actual'].astype(float).astype(int)).abs().mean()/accdf['revenue_actual'].astype(float).astype(int).abs()
            
            accdf['model_accuracy'] = 1.00 - accdf['model_accuracy']

    #np.mean(np.abs(forecast - actual)/np.abs(actual))
            accdf['model_accuracy'] = accdf['model_accuracy'].astype(float).round(2)
            finaldf = accdf[['date', 'data_category', 'data_source', 'model_name', 'travel_date','year', 'quarter', 'month', 'week', 'day', 'hour', 'region', 'origin','destination', 'flight', 'capacity', 'price_type', 'promotion','roundtrip_or_oneway', 'customer_type', 'product_type','location_lifestyle', 'location_economical_status','location_employment_status', 'location_event', 'source_precipitation', 'source_wind', 'destination_wind', 'source_humidity','destination_humidity', 'destination_precipitation', 'number_of_booking', 'currency', 'model_accuracy','accuracy_probability', 'revenue']]
            
            return finaldf
        
    def AccuracyBooking(self,Pred,test_data): 
            accdf = pd.concat([Pred,test_data['number_of_booking']],axis = 1)
            print(accdf.columns)
            accdf.columns= [['date', 'data_category', 'data_source', 'model_name', 'travel_date',
       'year', 'quarter', 'month', 'week', 'day', 'hour', 'region', 'origin',
       'destination', 'flight', 'capacity', 'price_type', 'promotion',
       'roundtrip_or_oneway', 'customer_type', 'product_type',
       'location_lifestyle', 'location_economical_status',
       'location_employment_status', 'location_event', 'source_precipitation',
       'source_wind', 'destination_wind', 'source_humidity',
       'destination_humidity', 'destination_precipitation', 'model_accuracy', 'accuracy_probability', 'number_of_booking','number_of_booking_actual']]
            print(accdf)
#            accdf['model_accuracy'] = ((accdf['revenue_actual'].astype(float).astype(int) - accdf['revenue'])**2).mean(0)**.5
#            accdf['model_accuracy'] = ((accdf['revenue_actual'].astype(float).astype(int).subtract(accdf['revenue']))**2).mean()**.5
            accdf['number_of_booking']=accdf['number_of_booking'].astype(float).astype(int)
            
#             accdf['model_accuracy'] = (accdf['number_of_booking'] - accdf['number_of_booking_actual']).abs().mean()/accdf['number_of_booking_actual']
            accdf['model_accuracy'] = (accdf['number_of_booking']-accdf['number_of_booking_actual']).abs().mean()/accdf['number_of_booking_actual']
        
            accdf['model_accuracy'] = 1 - accdf['model_accuracy']

    #np.mean(np.abs(forecast - actual)/np.abs(actual))
            accdf['model_accuracy'] = accdf['model_accuracy'].astype(float).round(2)
            finaldf = accdf[['date', 'data_category', 'data_source', 'model_name', 'travel_date','year', 'quarter', 'month', 'week', 'day', 'hour', 'region', 'origin','destination', 'flight', 'capacity', 'price_type', 'promotion','roundtrip_or_oneway', 'customer_type', 'product_type','location_lifestyle', 'location_economical_status','location_employment_status', 'location_event', 'source_precipitation', 'source_wind', 'destination_wind', 'source_humidity','destination_humidity', 'destination_precipitation', 'currency', 'model_accuracy','accuracy_probability','number_of_booking']]
            
            return finaldf
        
