Subfolder for Intra Year Attrition Prediction data extraction and processing

## Steps to Reproduce

### Virtual Enviroment

After cloning the repo, you will need to set up the virtual enviroment and install dependencies by running the following commands in the CLI in the folder:

* python -m venv env
* env/Scripts/activate
* pip install -r requirements.txt
* pip install ../../shared_packages/aws_helper_functions

### Setting Enviroment Vars & AWS Config

If run outside of lambda, applicable functions must be called with local_mode=True. Enviroment variables must be set for `host`, `database`, `port`, `username`, and `password` (eg redshift password) to connect to redshift. If writting results to S3 to update tables, AWS config must be set up w/`access key` and `secret access key`. 

## Files & Usage

* `intra_year_boosting.py` -> entry point. Leverages
  * `intra_year_attrition_training_lgbm.sql` -> query to create training data set
  * `intra_year_attrition_eval_lgbm.sql` -> query to create evaluation data set
* `env_setup/requirements.txt` -> packages that are requried to run intra_year_boosting (other than `aws_helper_functions`)