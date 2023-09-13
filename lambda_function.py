import intra_year_boosting
from datetime import date

def define_intrayear_attrition(run_date, query_dir, local_mode):
    df = intra_year_boosting.make_predictions(query_dir, local_mode)
    df['ds'] = run_date
    return df


def lambda_handler(event, context):
    local_mode = event.get('local_mode', False)
    query_dir = event.get('query_dir', '')
    run_date = event.get('run_date', str(date.today()))
    
    df = define_intrayear_attrition(run_date, query_dir, local_mode)
    if event.get('return_df_no_writes', False):
        return df

if __name__ == "__main__":
    lambda_handler({'local_mode':True, 'return_df_no_writes':True}, None)