# Imports
import numpy as np
import pandas as pd
import lightgbm as lgb
import shap
from aws_helper_functions import aws_helper_functions


def read_train_eval_data(query_dir, local_mode):
    # Designate file locations
    train_query_file = query_dir + 'intra_year_attrition_training_lgbm.sql'
    train_eval_file = query_dir + 'intra_year_attrition_eval_lgbm.sql'

    # Loading in fitting data for training and testing
    query = open(train_query_file).read()
    dat_train = aws_helper_functions.read_from_redshift(query, local_mode)

    # Loading in fitting data for evaluation
    query = open(train_eval_file).read()
    dat_eval = aws_helper_functions.read_from_redshift(query, local_mode)

    return dat_train, dat_eval

def clean_data_types(dat_train, dat_eval):
    df = pd.DataFrame(dat_train)
    df['commute'] = df['commute'].astype('int')
    df['school_name'] = df['school_name'].astype('category')
    df['scholar_grade'] = df['scholar_grade'].astype('category')
    df_eval = pd.DataFrame(dat_eval)
    df_eval['commute'] = df_eval['commute'].astype('int')
    df_eval['school_name'] = df_eval['school_name'].astype('category')
    df_eval['scholar_grade'] = df_eval['scholar_grade'].astype('category')
    return df_eval, df  

def clean_output_df(df_eval, y_pred_prob):
    results = pd.DataFrame(df_eval)
    results['predicted_attrition'] = y_pred
    results['attrition_probability'] = y_pred_prob
    results['scholar_grade'] = results['scholar_grade'].astype(str)
    results['scholar_grade'] = np.where(results['scholar_grade']=='0','K',results['scholar_grade'])
    results['commute_time'] = '0-10 min'
    results['commute_time'] = np.where(results['commute']==2,'10-20 min',results['commute_time'])
    results['commute_time'] = np.where(results['commute']==3,'20-30 min',results['commute_time'])
    results['commute_time'] = np.where(results['commute']==4,'30-40 min',results['commute_time'])
    results['commute_time'] = np.where(results['commute']==5,'40-50 min',results['commute_time'])
    results['commute_time'] = np.where(results['commute']==6,'50-60 min',results['commute_time'])
    results['commute_time'] = np.where(results['commute']==7,'>60 min',results['commute_time'])
    results['commute'] = results['commute_time']
    results['attrition_risk'] = '0-20%'
    results['attrition_risk'] = np.where(results['attrition_probability']>0.2,'20-40%',results['attrition_risk'])
    results['attrition_risk'] = np.where(results['attrition_probability']>0.4,'40-60%',results['attrition_risk'])
    results['attrition_risk'] = np.where(results['attrition_probability']>0.6,'60-80%',results['attrition_risk'])
    results['attrition_risk'] = np.where(results['attrition_probability']>0.8,'>80%',results['attrition_risk'])
    results = results[['sa_scholar_id','scholar_grade','school_name','address','commute',
    'new_scholar','gender_female','ell_status','sped_status','frpl_status','tardy_percent',
    'absent_percent','total_sus','total_rep','attrition_probability','attrition_risk']]
    return results

def make_predictions(query_dir, local_mode):
    dat_train, dat_eval = read_train_eval_data(query_dir, local_mode)
    dat_train, dat_eval = clean_data_types(dat_train, dat_eval)
    df_eval, df = clean_data_types(dat_train, dat_eval)

    X = df_eval
    X = X['new_scholar','scholar_grade', 'gender_female', 'ell_status', 'sped_status', 'tardy_percent', 'absent_percent', 'total_rep','total_sus','commute','school_name']
    X_train = df
    X_train = X_train['new_scholar','scholar_grade', 'gender_female', 'ell_status', 'sped_status', 'tardy_percent', 'absent_percent', 'total_rep','total_sus','commute','school_name']
    y_train = df['attrited']

    # Defining parameters for LightGBM
    params = {
        'objective': 'binary',
        'metric': 'binary_error',
        'verbosity': -1,
        'boosting_type': 'gbdt',
        'seed': 0,
        'feature_pre_filter': False,
        'lambda_l1': 0,
        'lambda_l2': 0,
        'num_leaves': 31,
        'feature_fraction': 1,
        'bagging_fraction': 1,
        'bagging_freq': 0,
        'min_child_samples': 20,
        'learning_rate': 0.1
        }

    # Fitting the model and predictions
    lgbm = lgb.LGBMClassifier(**params)
    lgbm.fit(X_train, y_train)
    y_pred_prob = lgbm.predict_proba(X)[:, 1]
    y_pred = lgbm.predict(X)
    
    results = clean_output_df(df_eval, y_pred_prob)

    # Explain LGBM predictions with Shap
    explainer = shap.TreeExplainer(lgbm)
    shap_values = explainer.shap_values(X)
    series_shap = pd.DataFrame(np.abs(shap_values[1]),columns = X.columns).apply(lambda x: x.nlargest(3).index.tolist(), axis=1)
    df_shap = pd.DataFrame(np.stack(series_shap),columns = ['first_driver','second_driver','third_driver'])
    results['first_driver'] = df_shap['first_driver']
    results['second_driver'] = df_shap['second_driver']
    results['third_driver'] = df_shap['third_driver']
    
    # Print outputs
    #results.to_csv('raw_data_science.raw_intra_year_predictions.csv')
    return results