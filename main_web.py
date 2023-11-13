from flask import Flask, request
import requests
import pandas as pd

app = Flask(__name__)

@app.route('/', methods=['POST'])
def result():
    # print(request.form['foo']) # should display 'bar'
    # return 'Received !' # response to your request.

    if request:
        # get record_id and record_id_prescreen from the main project
        data = {
            # 'token': main_token,
            'token': request.args.get('main_token'),
            'content': 'record',
            'action': 'export',
            'format': 'json',
            'type': 'flat',
            'csvDelimiter': '',
            'fields[0]': 'record_id',
            'fields[1]': 'record_id_prescreen',
            'rawOrLabel': 'raw',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'false',
            'exportSurveyFields': 'false',
            'exportDataAccessGroups': 'false',
            'returnFormat': 'json'
        }
        r = requests.post('https://redcap01.brisc.utah.edu/ccts/redcap/api/',data=data)
        #print('HTTP Status: ' + str(r.status_code))
        df_main = pd.DataFrame(r.json()).drop(columns=['redcap_event_name'])

        # filter out records with no prescreen id
        df_main_valid = df_main.loc[df_main['record_id_prescreen'] != '']

        # get all prescreen records
        data = {
            # 'token': pre_token,
            'token': request.args.get('pre_token'),
            'content': 'record',
            'action': 'export',
            'format': 'json',
            'type': 'flat',
            'csvDelimiter': '',
            'forms[0]': 'prescreen_survey',
            'rawOrLabel': 'raw',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'false',
            'exportSurveyFields': 'false',
            'exportDataAccessGroups': 'false',
            'returnFormat': 'json'
        }
        r = requests.post('https://redcap01.brisc.utah.edu/ccts/redcap/api/',data=data)
        #print('HTTP Status: ' + str(r.status_code))
        #print(r.json())
        df_prescreen = pd.DataFrame(r.json())

        df_pre_clean = df_prescreen.drop(columns=['ts','survey_duration_prescreen'])
        df_pre_clean = df_pre_clean.rename(columns={'record_id':'record_id_prescreen','prescreen_survey_complete':'prescreen_import_complete'})

        # merge records with prescreen id in main project with prescreen data
        df_import = df_main_valid.merge(df_pre_clean, how='left', on='record_id_prescreen')

        # convert to json
        df_import_json = df_import.to_json(orient='records')

        # import data into the main project
        data = {
            # 'token': main_token,
            'token': request.args.get('main_token'),
            'content': 'record',
            'action': 'import',
            'format': 'json',
            'type': 'flat',
            'overwriteBehavior': 'overwrite',
            'forceAutoNumber': 'false',
            'data': df_import_json,
            'returnContent': 'count',
            'returnFormat': 'json'
        }
        r = requests.post('https://redcap01.brisc.utah.edu/ccts/redcap/api/',data=data)
        # print('HTTP Status: ' + str(r.status_code))

if __name__ == '__main__':
    app.run(debug=True)