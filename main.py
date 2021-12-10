from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.openapi.utils import get_openapi
from fastapi.encoders import jsonable_encoder
import pandas as pd
import numpy as np
import re

app = FastAPI(title="DQ-API", description="Data Quality API", debug=True, version="1.0.0")


def cleaning(df):
    # Check email format

    email = list()
    for col in df.columns:
        for val in df[col].values:
            regex = r"[a-zA-Z0-9_+.-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
            if re.search(regex, str(val)):
                email.append(col)

    email = list(set(email))

    # Find Email Format
    bad_email_datalists = dict()
    for item in email:
        bad_email_datalists[item] = []

    good_email_datalists = dict()
    for item in email:
        good_email_datalists[item] = []

    # Replace email

    for col in email:
        for i, j in zip(df[col].values, df[col].index):
            if type(i) == str:
                x = re.findall(r'[a-zA-Z0-9_+.-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', i)
                if (x):
                    good_email_datalists[col].append(j)
                else:
                    bad_email_datalists[col].append(j)

    email_indexes = list()
    for col in email:
        email_indexes = email_indexes + bad_email_datalists[col]

    email_indexes = list(set(email_indexes))

    indexes = list()
    for col in email:
        indexes = indexes + good_email_datalists[col]

    indexes = list(set(indexes))

    # check mobile format

    mobile = list()
    for col in df.columns:
        for val in df[col].values:
            regex = r"\(\d{3}\) \d{3}-\d{4}"
            if re.search(regex, str(val)):
                mobile.append(col)

    mobile = list(set(mobile))

    phone_indexes = list()
    for i, j in zip(df[mobile[0]].values, df[mobile[0]].index):
        regex = r"\(\d{3}\) \d{3}-\d{4}"
        if re.search(regex, str(i)):
            pass
        else:
            phone_indexes.append(j)

    phone_indexes = df.loc[phone_indexes].dropna().index.values.tolist()

    # Replace Mobile number

    for i in range(len(df)):
        x = re.sub("[^0-9]", "", df[mobile[0]][i])
        if len(x) == 10:
            if df['Country'].values[i] == 'USA':
                regex = re.compile(r"([\d]{3})([\d]{3})([\d]{4})")
                df[mobile[0]][i] = re.sub(regex, r"+1 (\1) \2-\3", x)

            elif df['Country'].values[i] == 'France':
                regex = re.compile(r"([\d]{3})([\d]{3})([\d]{4})")
                df[mobile[0]][i] = re.sub(regex, r"+33 (\1) \2-\3", x)

            elif df['Country'].values[i] == 'India':
                regex = re.compile(r"([\d]{3})([\d]{3})([\d]{4})")
                df[mobile[0]][i] = re.sub(regex, r"+91 (\1) \2-\3", x)

            elif df['Country'].values[i] == 'UK':
                regex = re.compile(r"([\d]{3})([\d]{3})([\d]{4})")
                df[mobile[0]][i] = re.sub(regex, r"+44 (\1) \2-\3", x)

    # check date format

    date = list()
    for col in df.columns:
        for val in df[col].values:
            regex = r"\d{2}(/|-)\d{2}(/|-)\d{4}"
            if re.search(regex, str(val)):
                date.append(col)

    date = list(set(date))

    date_datalists = dict()
    for item in date:
        date_datalists[item] = []

    # Replace Date Format
    for col in date_datalists:
        for i, j in zip(df[col].values, df[col].index):
            regex = r"(\d{1}|\d{2})/(\d{1}|\d{2})/\d{4}"
            if re.search(regex, str(i)):
                pass
            else:
                date_datalists[col].append(j)

            df[col][j] = re.sub("[^0-9]", "/", str(i))

        for i, j in zip(df[col].values, df[col].index):
            if i == '///':
                df[col][j] = np.NaN
                date_datalists[col].remove(j)

    date_indexes = list()
    for col in date:
        date_indexes = date_indexes + date_datalists[col]

    date_indexes = list(set(date_indexes))

    # Remove Null Values

    nulls = []
    for val, ind in zip(df.isna().sum().values, df.isna().sum().index):
        if val > 1:
            for i, j in zip(df.isna()[ind].values, df.isna()[ind].index):
                if i:
                    nulls.append(j)

    df_null = df.iloc[list(set(nulls))]

    # Columns with Special Characters
    col_list = email + mobile + date

    str_col = list()
    for col in df.columns:
        if col not in col_list:
            if type(df[col].values[0]) == str:
                str_col.append(col)

    # Remove special character

    spc_datalists = dict()
    for item in str_col:
        spc_datalists[item] = []

    for z in str_col:
        for i, j in zip(df[z].values, df[z].index):
            result = re.findall('[^a-z0-9A-Z. ]', str(i))
            if result:
                spc_datalists[z].append(j)
                x = re.sub('[^a-z0-9A-Z. ]', '', str(i))
                df[z][j] = re.sub('\s+', ' ', x)

    spc_indexes = list()
    for col in str_col:
        spc_indexes = spc_indexes + spc_datalists[col]

    # Check Duplicates
    dup1 = []
    for i, j in zip(df.duplicated().values, df.duplicated().index.values):
        if i:
            dup1.append(j)

    # Bad data
    bad_data = list(set(dup1 + email_indexes + nulls))
    df_bad_data = df.iloc[bad_data]

    # Good Dataset
    df1 = df.loc[indexes]
    df1.drop_duplicates(inplace=True)
    df1.dropna(inplace=True)

    # Identify test data

    test_indexes = []
    indexes = []
    for i, j in zip(df1['Account Name'].values, df1['Account Name'].index):
        if type(i) == str:
            i = str.lower(i)
            x = re.findall(r'demo|test|sample', i)
            if (x):
                test_indexes.append(j)
            else:
                indexes.append(j)

    df_test = df1.loc[test_indexes]
    main_df = df1.loc[indexes]

    # Potential Dataset

    potential_golden = list(set(date_indexes + spc_indexes + phone_indexes))

    potential_golden_list = list()
    for i in potential_golden:
        for j in main_df.index.values.tolist():
            if i == j:
                potential_golden_list.append(i)

    df_golden = main_df[~main_df.index.isin(potential_golden_list)]
    df_potential_golden = main_df[main_df.index.isin(potential_golden_list)]

    data = pd.concat([df_golden, df_potential_golden, df_test, df_bad_data])
    data['Date'] = pd.to_datetime(data['Date'])

    data.insert(0, "Description", '')
    data.insert(1, "Remarks", '')

    data['Description'][0:len(df_golden)] = 'Golden Records'
    data['Description'][len(df_golden):len(df_golden) + len(df_potential_golden)] = 'Potential Golden Records'
    data['Description'][len(df_golden) + len(df_potential_golden):] = 'Faulty Records'

    data['Remarks'][len(df_golden):len(df_golden) + len(df_potential_golden)] = 'Changed to Golden Records'
    data['Remarks'][len(df_golden) + len(df_potential_golden):len(df_golden) + len(df_potential_golden) + len(
        df_test)] = 'Test Accounts'
    data['Remarks'][len(df_golden) + len(df_potential_golden) + len(df_test):] = 'Null and Dublicate Values'

    json_compatible_item_data = jsonable_encoder(data)
    
    return JSONResponse(content=json_compatible_item_data)


@app.post("/dq/uploadfile")
async def data_profiling(files: UploadFile = File(...)):
    df = pd.read_csv(files.file)
    df = df.fillna('')

    dataframe = cleaning(df=df)
    
    return dataframe



@app.get("/")
async def main():
    content = """
<body>
<form action="/csvdata/" enctype="multipart/form-data" method="post">
Test: <input name="files" type="file" multiple>
<input type="submit">
</form>
</body>
    """

    return HTMLResponse(content=content)


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="DQ-API",
        version="1.0.0",
        description="Data Quality API",
        routes=app.routes,
    )
    openapi_schema["info"]["x-logo"] = {
        "url": "https://43kw972g32dl1dbc5087i9gk-wpengine.netdna-ssl.com/wp-content/themes/Stralynn-WebSite/images/stralynn-logo-flat.svg"
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi
