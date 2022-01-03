from fbprophet import Prophet
import pandas as pd

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
            m = Prophet()
            m.fit(train_data_pr)
            future = m.make_future_dataframe(periods=1)
            prophet_pred = m.predict(future)
            prophet_pred = pd.DataFrame({"date" : prophet_pred[-50:]['ds'], "number_of_booking" : prophet_pred[-50:]["yhat"]}).reset_index(drop=True)
            test_data['model_name'] = 'PROPHET'
            test_data['data_source'] = 'MODEL'
            test_data['data_category'] = 'FORECAST'
            pred = pd.concat([test_data.drop(['number_of_booking'], axis=1).reset_index(drop=True), prophet_pred.drop(['date'], axis=1)], axis=1)
#             pred = pred[[test_data.columns]]
            print(pred)
            return pred
    
    def Model_train_revenue(self,train_data_pr,test_data_pr,test_data):
            m = Prophet()
            m.fit(train_data_pr)
            future = m.make_future_dataframe(periods=1)
            prophet_pred = m.predict(future)
            prophet_pred = pd.DataFrame({"date" : prophet_pred[-50:]['ds'], "revenue" : prophet_pred[-50:]["yhat"]}).reset_index(drop=True)
            test_data['model_name'] = 'PROPHET'
            test_data['data_source'] = 'MODEL'
            test_data['data_category'] = 'FORECAST'
            pred = pd.concat([test_data.drop(['revenue'], axis=1).reset_index(drop=True), prophet_pred.drop(['date'], axis=1)], axis=1)
            pred['revenue'] = pred['revenue'].astype(float).astype(int)
#             pred = pred[[test_data.columns]]
            print(pred)
            return pred