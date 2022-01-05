from redshift import RedshiftConnection,get_secret
import pandas as pd
import boto3
import json
import sys
from prophet import ProphetImplementation
from Arima import ArimaImplementation
from keras_impl_booking import KerasModelImplementationBooking
from keras_impl_revenue import KerasModelImplementationRevenue
import traceback
import warnings
warnings.filterwarnings("ignore")



def select_model(i):
    
    switcher={1:'ARIMA Model',2:'Keras Deep Learning- RNN Model',3:'Prophet Model',4:'DeepAR Model'}
    return switcher.get(i,"Invalid Model Number")

def select_forecast(i):
    
    switcher={1:'Revenue',2:'Booking/Sales'}
    return switcher.get(i,"Invalid Forecast Number")

if __name__=="__main__":
    try:
        print('.'*130)
        print('\n')
        print("Flydubai Demand Forecasting Demo: Select Model to Run\n")
        print('\t1. ARIMA Model\n\t2. Keras Deep Learning- RNN Model\n\t3. Prophet Model\n\t4. DeepAR Model\n')
        print('By Default, a Model will run for six months rolling forecast\n')
        print('.'*130)
        print('\n')
        secret = get_secret()
        username = secret['username']
        password = secret['password']
        engine = secret['engine']
        host = secret['host']
        port = secret['port']
        model_to_run = int(input('Enter the Model Number to Run [1-4]:\n'))
        if select_model(model_to_run)=='Keras Deep Learning- RNN Model':
            forecast_option = int(input('Enter the Forecasting Options  [1. Revenue, 2. Booking/Sales]:\n'))
            if select_forecast(forecast_option)=='Booking/Sales':
                redshift = RedshiftConnection(username,password,engine,host,port)
                print("Redshift Connection Established\n")
                #Truncate table with datasource as model
                truncate = RedshiftConnection.truncate_table_booking(redshift,"KERAS")
                print("Data has been truncated successfully\n")
                redshift = RedshiftConnection(username,password,engine,host,port)
                data = RedshiftConnection.read_redshift_data_booking(redshift)
                print("Passing redshift data to train the model\n")
                keras = KerasModelImplementationBooking()
                processed_data = keras.data_processing(data)
                print("Data Preprocessing Completed\n")
                X_train,X_test,y_train,y_test = keras.train_test_split(processed_data)
                print("Test, Train data splitted\n")
                trained_model = keras.train_model(X_train,y_train)
                y_test_pred = keras.predict_model(trained_model,X_test)
                model_outcome_df = keras.process_model_outcome(data,y_test_pred,username)
#                 print('Model outcome data - ',model_outcome_df)
                print("Successfully Model implemented\n")
                print("Inserting Model Outcome data into redshift\n")
                RedshiftConnection.write_redshift_booking(redshift,model_outcome_df)
                print("Model Outcome successfully inserted into redshift\n")       
            elif select_forecast(forecast_option)=='Revenue':
                redshift = RedshiftConnection(username,password,engine,host,port)
                print("Redshift Connection Established\n")
                #Truncate table with model name as keras
                truncate = RedshiftConnection.truncate_table_revenue(redshift,"KERAS")
                print("Data has been truncated successfully\n")
                redshift = RedshiftConnection(username,password,engine,host,port)
                data = RedshiftConnection.read_redshift_data_revenue(redshift)
                print("Passing redshift data to train the model\n")

                keras = KerasModelImplementationRevenue()
                processed_data = keras.data_processing(data)
                print("Data Preprocessing Completed\n")
                X_train,X_test,y_train,y_test = keras.train_test_split(processed_data)
                print("Test, Train data splitted\n")
                trained_model = keras.train_model(X_train,y_train)
                y_test_pred = keras.predict_model(trained_model,X_test)
                print("Successfully Model implemented\n")
                model_outcome_df = keras.process_model_outcome(data,y_test_pred,username)
                #Add missing column values - number_of_booking,region,roundtrip_or_oneway
                print("Inserting Model Outcome data into redshift\n")
                RedshiftConnection.write_redshift_revenue(redshift,model_outcome_df)
                print("Model Outcome successfully inserted into redshift\n")
            elif select_forecast(forecast_option)=='Invalid Forecast Number':
                print("Please Enter Valid Forecast Number")
                      
        if select_model(model_to_run)=='ARIMA Model':
            forecast_option = int(input('Enter the Forecasting Options  [1. Revenue, 2. Booking/Sales]:\n'))
            if select_forecast(forecast_option)=='Booking/Sales':
                redshift = RedshiftConnection(username,password,engine,host,port)
                print("Redshift Connection Established\n")
                #Truncate table with datasource as model
                truncate = RedshiftConnection.truncate_table_booking(redshift,"ARIMA")
                print("Data has been truncated successfully\n")
                redshift = RedshiftConnection(username,password,engine,host,port)
                data = RedshiftConnection.read_redshift_data_booking(redshift)
                print("Passing redshift data to train the model\n")
                print(data)
                arima = ArimaImplementation()
                processed_data = arima.Datapreprocessing(data)
                print("Data Preprocessing Completed\n")
                Pred,test_data = arima.Model_train_booking(processed_data)
                print('Model outcome data - ',Pred)
#                 finaldf = arima.AccuracyBooking(Pred,test_data)
#                 print('Model outcome data - ',finaldf)
                print("Inserting Model Outcome data into redshift\n")
                RedshiftConnection.write_redshift_booking(redshift,Pred)
                print("Model Outcome successfully inserted into redshift\n")

            elif select_forecast(forecast_option)=='Revenue':
                    redshift = RedshiftConnection(username,password,engine,host,port)
                    print("Redshift Connection Established\n")
                    #Truncate table with datasource as model
                    truncate = RedshiftConnection.truncate_table_revenue(redshift,"ARIMA")
                    print("Data has been truncated successfully\n")
                    redshift = RedshiftConnection(username,password,engine,host,port)
                    data = RedshiftConnection.read_redshift_data_revenue(redshift)
                    print("Passing redshift data to train the model\n")

                    arima = ArimaImplementation()
                    processed_data = arima.Datapreprocessing(data)
                    print("Data Preprocessing Completed\n")
                    Pred,test_data = arima.Model_train_revenue(processed_data)
                    print('Model outcome data - ',Pred)
                    finaldf = arima.AccuracyRevenue(Pred,test_data)
                    print('Model outcome data - ',finaldf)
                    print("Inserting Model Outcome data into redshift\n")
                    RedshiftConnection.write_redshift_revenue(redshift,finaldf)
                    print("Model Outcome successfully inserted into redshift\n")
                    
            elif select_forecast(forecast_option)=='Invalid Forecast Number':
                print("Please Enter Valid Forecast Number")

                    
#              elif sys.argv[1]=="DEEPAR":
#                  if sys.argv[2]=="BOOKING":
#                     data = RedshiftConnection.read_redshift_data(redshift)
#                     print("Passing redshift data to train the model")

#                     print(data)
#                     #Model Implementation
#                     #df = pd.DataFrame({'description':['GBP','YEN'],'id':[3,4]})
#                     #data2 = RedshiftConnection.write_redshift(redshift,df)
                    
#                 elif sys.argv[2]=="REVENUE":
#                     data = RedshiftConnection.read_redshift_data(redshift)
#                     print("Passing redshift data to train the model")

#                     print(data)
#                     #Model Implementation
#                     #df = pd.DataFrame({'description':['GBP','YEN'],'id':[3,4]})
#                     #data2 = RedshiftConnection.write_redshift(redshift,df)
                    
        if select_model(model_to_run)=='Prophet Model':
            forecast_option = int(input('Enter the Forecasting Options  [1. Revenue, 2. Booking/Sales]:\n'))
            if select_forecast(forecast_option)=='Booking/Sales':
                redshift = RedshiftConnection(username,password,engine,host,port)
                print("Redshift Connection Established\n")
                #Truncate table with datasource as model
                truncate = RedshiftConnection.truncate_table_booking(redshift,"PROPHET")
                print("Data has been truncated successfully\n")
                redshift = RedshiftConnection(username,password,engine,host,port)
                data = RedshiftConnection.read_redshift_data_booking(redshift)
                print("Passing redshift data to train the model\n")
                print(data)
                prop = ProphetImplementation()
                train_data_pr,test_data_pr,test_data = prop.Datapreprocessingbooking(data)
                print("Data Preprocessing Completed\n")
                pred = prop.Model_train_booking(train_data_pr,test_data_pr,test_data)
                print('Model outcome data - ',pred)
                RedshiftConnection.write_redshift_booking(redshift,pred)
                print("Model Outcome successfully inserted into redshift\n")
                    
            elif select_forecast(forecast_option)=="Revenue":
                redshift = RedshiftConnection(username,password,engine,host,port)
                print("Redshift Connection Established\n")
                #Truncate table with datasource as model
                truncate = RedshiftConnection.truncate_table_revenue(redshift,"PROPHET")
                print("Data has been truncated successfully\n")
                redshift = RedshiftConnection(username,password,engine,host,port)
                data = RedshiftConnection.read_redshift_data_revenue(redshift)
                print("Passing redshift data to train the model\n")
                print(data)
                prop = ProphetImplementation()
                train_data_pr,test_data_pr,test_data = prop.DatapreprocessingRevenue(data)
                print("Data Preprocessing Completed\n")
                pred,test_data = prop.Model_train_revenue(train_data_pr,test_data_pr,test_data)
                print('Model outcome data - ',pred)
                finaldf = prop.AccuracyRevenue(pred,test_data)
                print('Model outcome data - ',finaldf)
                RedshiftConnection.write_redshift_revenue(redshift,finaldf)
                print("Model Outcome successfully inserted into redshift\n") 
                
            elif select_forecast(forecast_option)=='Invalid Forecast Number':
                print("Please Enter Valid Forecast Number")
            
        elif select_model(model_to_run)=='Invalid Model Number':
            print("Please Enter Valid Model Number")
            
            
    except BaseException as err:
        
        print('In Error Block - ',traceback.print_exc())
