from assignment_12 import *
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.metrics import accuracy_score
import lime
import lime.lime_tabular
from lime import submodular_pick
import shap
import matplotlib.pyplot as plt
import numpy as np
# convert lst helper function
def convert_lst(lst):
    newlist = []
    for each in lst:     
        if each[0] == 0:
            newlist.append(('month',each[1]))
        elif each[0] == 1:
            newlist.append(('day',each[1]))
        elif each[0] == 2:
            newlist.append(( 'hour',each[1]))
        elif each[0] == 3:
            newlist.append(( 'minute',each[1]))
        elif each[0] == 4:
            newlist.append(('siteid',each[1]))
        elif each[0] == 5:
            newlist.append(('offerid',each[1]))
        elif each[0] == 6:
            newlist.append(('category',each[1]))
        elif each[0] == 7:
            newlist.append(('merchant',each[1]))
        elif each[0] == 8:
            newlist.append(('countrycode',each[1]))
        elif each[0] == 9:
            newlist.append(('browserid',each[1]))
        elif each[0] == 10:
            newlist.append(('devid',each[1]))
    return newlist


# a user-defined mapping function
def custom_mapping(atuple):
    filtered_zero = list(map(lambda x: 0 if x == 'nan' else x, atuple.tuple)) # filter out missing value
    if filtered_zero[6] != 0: # if we have a valid countrycode
        country_code = filtered_zero[6].lower()
        filtered_zero[6] = hash(country_code)%10000000
    # if we have a valid devid
    if filtered_zero[8] != 0:
        devid = filtered_zero[8].lower()
        filtered_zero[8] = hash(devid)%10000000
    if filtered_zero[7] != 0: # if we have a valid browser id
        browser_str = filtered_zero[7].lower()
        # clean browser id
        if browser_str == "mozilla firefox" or browser_str== "mozilla":
            filtered_zero[7] = "Firefox"
        if browser_str== "internet explorer" or browser_str== "internetexplorer":
            filtered_zero[7] = "IE"
        if browser_str == "google chrome":
            filtered_zero[7] = "Chrome"
        # transform browser id to a hash value
        hash_val = hash(filtered_zero[7])%10000000
        filtered_zero[7] = hash_val
    # transform datetime = four new attributes
    if filtered_zero[1] != 0:
        [date, time] = filtered_zero[1].split(' ') # split date and exact time
        [year,month,day] = date.split('-')
        [hour,minute,second] = time.split(':')
        # convert to int values
        month = int(month)
        day = int(day)
        hour = int(hour)
        minute = int(minute)
    # create a new tuple containing the new mapped data
    newTuple = [month,day,hour,minute,int(filtered_zero[2]),int(filtered_zero[3]),int(filtered_zero[4]),int(filtered_zero[5]),filtered_zero[6],filtered_zero[7],filtered_zero[8],int(filtered_zero[9])]
    return newTuple


if __name__ == "__main__":

    #build CLI
    parser = argparse.ArgumentParser(description="CS591L1 Assignment #3\nrunTask #1: missing values for every attributes of table 1 and distinct values for attributes countrycode, browserid and devid\nrunTask #2: Model Training and print report of accuracy \nrunTask #3: output LIME and SHAP explanations for the model we trained.", formatter_class=RawTextHelpFormatter)
    parser.add_argument("-t", "--task", metavar="[task_number]", type=int, required=True, help="Task # to run", dest="task_num")
    parser.add_argument("-d", "--data", metavar="[path_to_dataset]", type=str, required=True, help="Path to dataset", dest="dataset")
    
    args = parser.parse_args()
    filepathInfo = "filepath to dataset={}".format(args.dataset)
    print(filepathInfo)
    def runTask1():
        print('Executing Task 1')
        missing_arr = []
        testScan = Scan(filepath = args.dataset)
        for i in range(0,10):
            testScan.curr=0
            testScan.end_of_file=False
            testProject = Project(input=testScan,fields_to_keep=[i])
            while True:
                batch = testProject.get_next()
                if batch == None:
                    break
            if(i == 0):
                missing_arr.append(('ID',testProject.getCountMissing()))
            elif(i == 1):
                missing_arr.append(('datetime',testProject.getCountMissing()))
            elif(i == 2):
                missing_arr.append(('siteid',testProject.getCountMissing()))
            elif(i == 3):
                missing_arr.append(('offerid',testProject.getCountMissing()))
            elif(i == 4):
                missing_arr.append(('category',testProject.getCountMissing()))
            elif(i == 5):
                missing_arr.append(('merchant',testProject.getCountMissing()))
            elif(i == 6):
                missing_arr.append(('countrycode',testProject.getCountMissing()))
            elif(i == 7):
                missing_arr.append(('browserid',testProject.getCountMissing()))
            elif(i == 8):
                missing_arr.append(('devid',testProject.getCountMissing()))
            elif(i == 9):
                missing_arr.append(('click',testProject.getCountMissing()))
        print("missing values for every atrributes in the dataset:")
        print(missing_arr)
        # get distinct value for devid, browserid and countrycode
        distinct_arr = []
        for i in range(0,3):
            testScan.curr=0
            testScan.end_of_file=False
            testProject = Project(input=testScan,fields_to_keep=[i+6])
            testDistinct = Distinct(input = testProject,attr_to_distinct=0)
            while True:
                batch = testDistinct.get_next()
                if batch == None:
                    break
            if(i == 0):
                distinct_arr.append(('countrycode',testDistinct.getSetSize()-1))
            elif(i == 1):
                distinct_arr.append(('browserid',testDistinct.getSetSize()-1))
            elif(i == 2):
                distinct_arr.append(('devid',testDistinct.getSetSize()-1))
        print("Distinct values (not including empty value) for countrycode, browserid, and devid attributes:")
        print(distinct_arr)
        print("Job finished")

    # data preprocessing and model training 
    def runTask2():
        print('Executing Task 3')
        print('Data preprocessing...')
        # first we need to clean the data
        testScan = Scan(filepath= args.dataset)


        #task 3 
        testMap = Map(input = testScan,map_func =custom_mapping)
        cleaned_lst = []
        while True:
            batch = testMap.get_next()
            if batch == None:
                break
            cleaned_lst += batch

        #convert list to dataframe
        df = pandas.DataFrame(cleaned_lst, columns = ['month','day','hour','minute','siteid','offerid','category','merchant','countrycode','browserid','devid','click'])
        # declare feature vector and target variable
        features = ['month','day','hour','minute','siteid','offerid','category','merchant','countrycode','browserid','devid']
        X = df[features]
        Y = df['click']

        # split dataset into training and testing set
        x_train, x_test, y_train, y_test = train_test_split(X ,Y , test_size = 0.3, random_state = 0)
        print('training the model...')
        #train the model
        gbm = lgb.LGBMClassifier()

        gbm.fit(x_train,y_train)
        # predict
        y_pred = gbm.predict(x_test)
        # validate and report accuracy
        print("Accuracy Score:")
        print(accuracy_score(y_test,y_pred))
        print("Classification report:")
        print(classification_report(y_test,y_pred))
        print('Job finished')


    # task 4
    def runTask3():
        print('Executing Task 4')
        #retrieve the model from task 3
        # first we need to clean the data
        print('data preprocessing')
        testScan = Scan(filepath=args.dataset)

        testMap = Map(input = testScan,map_func =custom_mapping)
        cleaned_lst = []
        while True:
            batch = testMap.get_next()
            if batch == None:
                break
            cleaned_lst += batch

        #convert list to dataframe
        df = pandas.DataFrame(cleaned_lst, columns = ['month','day','hour','minute','siteid','offerid','category','merchant','countrycode','browserid','devid','click'])
        # declare feature vector and target variable
        features = ['month','day','hour','minute','siteid','offerid','category','merchant','countrycode','browserid','devid']
        X = df[features]
        Y = df['click']

        # split dataset into training and testing set
        x_train, x_test, y_train, y_test = train_test_split(X ,Y , test_size = 0.3, random_state = 0)
        print('training the model...')
        #train the model
        gbm = lgb.LGBMClassifier()

        gbm.fit(x_train,y_train)
        

       

        # find a test instances that generate the prediction y = 0 and y = 1
        x_var0 = None
        x_var0_idx = -1
        x_var1 = None
        x_var1_idx = -1
        for each in x_test.values:
            x_var0_idx += 1
            if gbm.predict(each.reshape(1,-1)) == [[0]]:
                x_var0 = each
                break
        for each in x_test.values:
            x_var1_idx = -1
            if gbm.predict(each.reshape(1,-1)) == [[1]]:
                x_var1 = each
                break
        print('outputing explanations')
        # we execute fourth sub-task first because of matplotlib issue
        shap.initjs()

        #data for prediction, retrieve row information for two different predictions
        val0_arr = x_test.iloc[x_var0_idx,:]
        val1_arr = x_test.iloc[x_var1_idx,:]
        # explain the model's predictions using SHAP
        shap_explainer = shap.TreeExplainer(gbm)
        shap_values = shap_explainer.shap_values(x_test)
        # pull out SHAP values for positive outcomes and evaluate features that contribute to that

        #fourth sub-task

        fig = shap.summary_plot(shap_values[1],x_test,show = False,max_display = 12,plot_type = 'dot',plot_size=(30.0,20.0))

        plt.savefig('../task4_image/shap3.png')
        print('SHAP summary plot generated')
         #first sub-task
        #generate an explanation
        # LIME Explainer 1
        lime_explainer = lime.lime_tabular.LimeTabularExplainer(x_train.values,mode = 'classification',feature_names = features,class_names = ['not click','click'], verbose = False)

        def predict_fn(x):
            preds = gbm.predict(x).reshape(-1,1)
            p0 = 1 - preds
            return np.hstack((p0,preds))



        exp0 = lime_explainer.explain_instance(x_var0,predict_fn,num_features = 11)
        exp1 = lime_explainer.explain_instance(x_var1,predict_fn,num_features = 11)
        # plot graph
        fig0 = exp0.as_pyplot_figure()
        fig0.set_size_inches(25,22)
        fig0.savefig('../task4_image/lime1.png',dpi = 100)
        print('LIME graph 1 for instance predict value y = 0 generated')
        fig1 = exp1.as_pyplot_figure()
        fig1.set_size_inches(25,22)
        fig1.savefig('../task4_image/lime2.png',dpi = 100)
        print('LIME graph 2 for instance predict value y = 1 generated')

        # second sub-task

        #forceplot for predict value y = 0
        shap.force_plot(shap_explainer.expected_value[1],shap_values[1][x_var0_idx,:],val0_arr,show=False,matplotlib=True).savefig('../task4_image/shap1.png',dpi = 100)
        print('SHAP graph 1 for instance predict value y = 0 generated')
        # forceplot for predict value y = 1
        shap.force_plot(shap_explainer.expected_value[1],shap_values[1][x_var1_idx,:],val1_arr,show=False,matplotlib=True).savefig('../task4_image/shap2.png',dpi = 100)
        print('SHAP graph 2 for instance predict value y = 1 generated')

        #third sub-task
        # pick 10 instances
        #x_train_tolist = [x_train.columns.values.tolist()]+x_train.values.tolist()
        sp_obj = submodular_pick.SubmodularPick(lime_explainer, x_train.to_numpy(),gbm.predict_proba,sample_size = 10,num_features=11,top_labels = 0)
        
        print('global explanation using submodular pick LIME')
        print('generating 10 instances...')
        print('feature 0: month, feature 1: day, feature 2: hour, feature 3: minute, feature 4: siteid, feature 5: offerid, feature 6: category, feature 7: merchant,feature 8: countrycode, feature 9: browserid, feature 10: devid')
        idx = 0
        for each in sp_obj.explanations:
            print('Generating instance'+str(idx))
            lst = each.as_map()[1]
            lst = convert_lst(lst)
            print(lst)
            idx += 1
        print('Job finished.')

    eval("runTask" + str(args.task_num))()
