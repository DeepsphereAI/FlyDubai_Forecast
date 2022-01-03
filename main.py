from redshift import RedshiftConnection,get_secret
import pandas as pd
import boto3
import json
import sys
from prophet import ProphetImplementation
from Arima import ArimaImplementation
from keras_impl import KerasModelImplementation
import traceback

if __name__=="__main__":
    try:
        if len(sys.argv)==4 or len(sys.argv)==3:
            secret = get_secret()
            username = secret['username']
            password = secret['password']
            engine = secret['engine']
            host = secret['host']
            port = secret['port']
            
            if sys.argv[1].upper()=="KERAS":
                if sys.argv[2].upper()=="BOOKING":
                    redshift = RedshiftConnection(username,password,engine,host,port)
                    print("Redshift Connection Established")
                    #Truncate table with datasource as model
                    truncate = RedshiftConnection.truncate_table(redshift,"KERAS")
                    print("Data has been truncated successfully")
                    redshift = RedshiftConnection(username,password,engine,host,port)
                    data = RedshiftConnection.read_redshift_keras(redshift)
                    print("Passing redshift data to train the model")
                    print(data)
                    keras = KerasModelImplementation()
                    processed_data = keras.data_processing(data)
                    print("Data Preprocessing Completed")
                    X_train,X_test,y_train,y_test = keras.train_test_split(processed_data)
                    print("Test, Train data splitted")
                    trained_model = keras.train_model(X_train,y_train)
                    y_test_pred = keras.predict_model(trained_model,X_test)
                    model_outcome_df = keras.process_model_outcome(data,y_test_pred,username)
                    print('Model outcome data - ',model_outcome_df)
                    print("Successfully Model implemented")
                    RedshiftConnection.write_keras_redshift(redshift,model_outcome_df)
                    print("Model Outcome successfully inserted into redshift")
                    
                elif sys.argv[2].upper()=="REVENUE":
                    data = RedshiftConnection.read_redshift_data(redshift)
                    print("Passing redshift data to train the model")
                    print(data)
                      
            elif sys.argv[1]=="ARIMA":
                if sys.argv[2]=="BOOKING":
                    redshift = RedshiftConnection(username,password,engine,host,port)
                    print("Redshift Connection Established")
                    #Truncate table with datasource as model
                    truncate = RedshiftConnection.truncate_table_booking(redshift,"ARIMA")
                    print("Data has been truncated successfully")
                    redshift = RedshiftConnection(username,password,engine,host,port)
                    data = RedshiftConnection.read_redshift_data_booking(redshift)
                    print("Passing redshift data to train the model")
                    print(data)
                    arima = ArimaImplementation()
                    processed_data = arima.Datapreprocessing(data)
                    print("Data Preprocessing Completed")
                    test_data = arima.Model_train_booking(processed_data)
                    print('Model outcome data - ',test_data)
#                     outcome = arima.Model_outcome(data,arima_pred,username)
                    
                    RedshiftConnection.write_redshift_booking(redshift,test_data)
                    print("Model Outcome successfully inserted into redshift")

                elif sys.argv[2]=="REVENUE":
                    redshift = RedshiftConnection(username,password,engine,host,port)
                    print("Redshift Connection Established")
                    #Truncate table with datasource as model
                    truncate = RedshiftConnection.truncate_table_revenue(redshift,"ARIMA")
                    print("Data has been truncated successfully")
                    redshift = RedshiftConnection(username,password,engine,host,port)
                    data = RedshiftConnection.read_redshift_data_revenue(redshift)
                    print("Passing redshift data to train the model")

                    arima = ArimaImplementation()
                    processed_data = arima.Datapreprocessing(data)
                    print("Data Preprocessing Completed")
                    test_data = arima.Model_train_revenue(processed_data)
                    print('Model outcome data - ',test_data)
#                     outcome = arima.Model_outcome(data,arima_pred,username)
                    
                    RedshiftConnection.write_redshift_revenue(redshift,test_data)
                    print("Model Outcome successfully inserted into redshift")

                    #Model Implementation
                    #df = pd.DataFrame({'description':['GBP','YEN'],'id':[3,4]})
                    #data2 = RedshiftConnection.write_redshift(redshift,df)
                    
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
                    
            elif sys.argv[1]=="PROPHET":
                if sys.argv[2]=="BOOKING":
                    redshift = RedshiftConnection(username,password,engine,host,port)
                    print("Redshift Connection Established")
                    #Truncate table with datasource as model
                    truncate = RedshiftConnection.truncate_table_booking(redshift,"PROPHET")
                    print("Data has been truncated successfully")
                    redshift = RedshiftConnection(username,password,engine,host,port)
                    data = RedshiftConnection.read_redshift_data_booking(redshift)
                    print("Passing redshift data to train the model")
                    print(data)
                    prop = ProphetImplementation()
                    train_data_pr,test_data_pr,test_data = prop.Datapreprocessingbooking(data)
                    print("Data Preprocessing Completed")
                    pred = prop.Model_train_booking(train_data_pr,test_data_pr,test_data)
                    print('Model outcome data - ',pred)
                    RedshiftConnection.write_redshift_booking(redshift,pred)
                    print("Model Outcome successfully inserted into redshift")
                    
                elif sys.argv[2]=="REVENUE":
                    redshift = RedshiftConnection(username,password,engine,host,port)
                    print("Redshift Connection Established")
                    #Truncate table with datasource as model
                    truncate = RedshiftConnection.truncate_table_revenue(redshift,"PROPHET")
                    print("Data has been truncated successfully")
                    redshift = RedshiftConnection(username,password,engine,host,port)
                    data = RedshiftConnection.read_redshift_data_revenue(redshift)
                    print("Passing redshift data to train the model")
                    print(data)
                    prop = ProphetImplementation()
                    train_data_pr,test_data_pr,test_data = prop.DatapreprocessingRevenue(data)
                    print("Data Preprocessing Completed")
                    pred = prop.Model_train_revenue(train_data_pr,test_data_pr,test_data)
                    print('Model outcome data - ',pred)
                    RedshiftConnection.write_redshift_revenue(redshift,pred)
                    print("Model Outcome successfully inserted into redshift")            
            
        elif len(sys.argv)<3:
            print("Please Enter Required Command line arguments:\n1.MODEL NAME\n2.FORECASTING NAME\n3.FORECASTING PERIOD(OPTIONAL - DEFAULT VALUE JAN 2022-JUN 2022)")
            
            
    except BaseException as err:
        
        print('In Error Block - ',traceback.print_exc())
