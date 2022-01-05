from fbprophet import Prophet
import pandas as pd
import numpy as np

class ProphetImplementation:
    def Datapreprocessingbooking(self,data):
            data.date = pd.to_datetime(data.date)
            train_data = data.iloc[:len(data)-50]
            test_data = data.iloc[len(data)-50:]
            train_data_pr = train_data[['date', 'number_of_booking']]
            test_data_pr = test_data[['date', 'number_of_booking']]
            train_data_pr.columns = ['ds','y']
            test_data_pr.columns = ['ds','y']# To use prophet column names should be like that
#             train_data_pr = df_pr.iloc[:len(df_pr)-50]
#             test_data_pr = df_pr.iloc[len(df_pr)-50:]
            
            return train_data_pr,test_data_pr,test_data
        
    def DatapreprocessingRevenue(self,data):
            data.date = pd.to_datetime(data.date)
            train_data = data.iloc[:len(data)-50]
            test_data = data.iloc[len(data)-50:]
            train_data_pr = train_data[['date', 'revenue']]
            test_data_pr = test_data[['date', 'revenue']]
            train_data_pr.columns = ['ds','y']
            test_data_pr.columns = ['ds','y']# To use prophet column names should be like that
#             train_data_pr = df_pr.iloc[:len(df_pr)-50]
#             test_data_pr = df_pr.iloc[len(df_pr)-50:]
            return train_data_pr,test_data_pr,test_data
        
    def Model_train_booking(self,train_data_pr,test_data_pr,test_data):
            accuracy_list = list(np.random.random_sample((50,)))
            prob_list = list(np.random.random_sample((50,)))
            m = Prophet()
#             accuracy_list = list(np.random.random_sample((768,)))
#             prob_list = list(np.random.random_sample((768,)))
            m.fit(train_data_pr)
            future = m.make_future_dataframe(periods=1)
            prophet_pred = m.predict(future)
            prophet_pred = pd.DataFrame({"date" : prophet_pred[-50:]['ds'], "number_of_booking" : prophet_pred[-50:]["yhat"]}).reset_index(drop=True)
            test_data['model_name'] = 'PROPHET'
            test_data['data_source'] = 'MODEL'
            test_data['data_category'] = 'FORECAST'
            test_data['model_accuracy'] = [round(num, 2) for num in accuracy_list]
            test_data['accuracy_probability'] = [round(num, 2) for num in prob_list]
#             test_data['model_accuracy'] = [round(num, 2) for num in accuracy_list]
#             test_data['accuracy_probability'] = [round(num, 2) for num in prob_list]
            pred = pd.concat([test_data.drop(['number_of_booking'], axis=1).reset_index(drop=True), prophet_pred.drop(['date'], axis=1)], axis=1)
#             pred = pred[[test_data.columns]]
            print(pred)
            return pred
    
    def Model_train_revenue(self,train_data_pr,test_data_pr,test_data):
            accuracy_list = list(np.random.random_sample((50,)))
            prob_list = list(np.random.random_sample((50,)))
            m = Prophet()
            m.fit(train_data_pr)
            future = m.make_future_dataframe(periods=1)
            prophet_pred = m.predict(future)
            prophet_pred = pd.DataFrame({"date" : prophet_pred[-50:]['ds'], "revenue" : prophet_pred[-50:]["yhat"]}).reset_index(drop=True)
            test_data['model_name'] = 'PROPHET'
            test_data['data_source'] = 'MODEL'
            test_data['data_category'] = 'FORECAST'
            pred = pd.concat([test_data.drop(['revenue'], axis=1).reset_index(drop=True), prophet_pred.drop(['date'], axis=1)], axis=1)
            pred['model_accuracy'] = [round(num, 2) for num in accuracy_list]
            pred['accuracy_probability'] = [round(num, 2) for num in prob_list]
            print(pred.columns)
            pred = pred.dropna()
            pred['revenue'] = pred['revenue'].astype(float).astype(int)
            
#             test_data['model_accuracy'] = [round(num, 2) for num in accuracy_list]
#             test_data['accuracy_probability'] = [round(num, 2) for num in prob_list]
#             pred = pred[[test_data.columns]]
            print(pred)
            return pred,test_data
    
    def AccuracyRevenue(self,Pred,test_data): 
            accdf = pd.concat([Pred,test_data['revenue']],axis = 1)
            print(accdf.columns)
            accdf.columns= ['data_category', 'data_source', 'model_name', 'travel_date', 'year', 'quarter', 'month', 'week', 'day', 'hour', 'region', 'origin','destination', 'flight', 'capacity', 'price_type', 'promotion','roundtrip_or_oneway', 'customer_type', 'product_type','location_lifestyle', 'location_economical_status', 'location_employment_status', 'location_event', 'source_precipitation','source_wind', 'destination_wind', 'source_humidity',
       'destination_humidity', 'destination_precipitation',
       'number_of_booking', 'currency', 'date', 'model_accuracy',
       'accuracy_probability', 'revenue','revenue_actual']
            print(accdf)
#             accdf = accdf.dropna()
#            accdf['model_accuracy'] = ((accdf['revenue_actual'].astype(float).astype(int) - accdf['revenue'])**2).mean(0)**.5
#            accdf['model_accuracy'] = ((accdf['revenue_actual'].astype(float).astype(int).subtract(accdf['revenue']))**2).mean()**.5
#             accdf['model_accuracy'] = np.sqrt(((accdf['revenue_actual'].astype(float).astype(int) - accdf['revenue'])**2).expanding().mean())
            print(accdf.dtypes)
            
            accdf = accdf.dropna(subset = ['revenue'])
            print(accdf.isnull().sum())
            accdf['model_accuracy'] = (accdf['revenue'] - accdf['revenue_actual'].astype(float).astype(int)).abs().mean()/accdf['revenue_actual'].astype(float).astype(int).abs()
            
            accdf['model_accuracy'] = 1.00 - accdf['model_accuracy']

    #np.mean(np.abs(forecast - actual)/np.abs(actual))
            accdf['model_accuracy'] = accdf['model_accuracy'].astype(float).round(2)
            finaldf = accdf[['date', 'data_category', 'data_source', 'model_name', 'travel_date','year', 'quarter', 'month', 'week', 'day', 'hour', 'region', 'origin','destination', 'flight', 'capacity', 'price_type', 'promotion','roundtrip_or_oneway', 'customer_type', 'product_type','location_lifestyle', 'location_economical_status','location_employment_status', 'location_event', 'source_precipitation', 'source_wind', 'destination_wind', 'source_humidity','destination_humidity', 'destination_precipitation', 'number_of_booking', 'currency', 'model_accuracy','accuracy_probability', 'revenue']]
            
            return finaldf