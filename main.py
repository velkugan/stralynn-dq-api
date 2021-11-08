from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.openapi.utils import get_openapi
from fastapi.encoders import jsonable_encoder
import pandas as pd
import re

app = FastAPI(title="DQ-API", description="Data Quality API", debug=True, version="1.0.0")


@app.post("/dq/uploadfile")
async def data_profiling(files: UploadFile = File(...)):
    df = pd.read_csv(files.file)
    df = df.fillna('')

    dup1 = []
    for i, j in zip(df.duplicated().values, df.duplicated().index.values):
        if i:
            dup1.append(j)

        df_dup1 = df.loc[dup1]

        for i, j in zip(df['Account Name'].values, df['Account Name'].index):
            x = re.sub('[^a-z0-9A-Z ]', '', str(i))
            df['Account Name'][j] = re.sub('\s+', ' ', x)

        email_indexes = []
        indexes = []

        for i, j in zip(df['Email'].values, df['Email'].index):
            if type(i) == str:
                x = re.findall(r'[a-zA-Z0-9_+.-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', i)

                if (x):
                    indexes.append(j)
                else:
                    email_indexes.append(j)

        df_emails = df.iloc[email_indexes]

        for i in range(len(df)):
            x = re.sub("[^0-9]", "", df['Phone'][i])

            if len(x) == 10:

                if df['Country'].values[i] == 'USA':
                    regex = re.compile(r"([\d]{3})([\d]{3})([\d]{4})")
                    df['Phone'][i] = re.sub(regex, r"+1 (\1) \2-\3", x)

                elif df['Country'].values[i] == 'France':
                    regex = re.compile(r"([\d]{3})([\d]{3})([\d]{4})")
                    df['Phone'][i] = re.sub(regex, r"+33 (\1) \2-\3", x)

                elif df['Country'].values[i] == 'India':
                    regex = re.compile(r"([\d]{3})([\d]{3})([\d]{4})")
                    df['Phone'][i] = re.sub(regex, r"+91 (\1) \2-\3", x)

                elif df['Country'].values[i] == 'UK':
                    regex = re.compile(r"([\d]{3})([\d]{3})([\d]{4})")
                    df['Phone'][i] = re.sub(regex, r"+44 (\1) \2-\3", x)

        nulls = []
        for val, ind in zip(df.isna().sum().values, df.isna().sum().index):
            if val > 1:
                for i, j in zip(df.isna()[ind].values, df.isna()[ind].index):
                    if i:
                        nulls.append(j)

        null_df = df.iloc[list(set(nulls))]

        data_copy = pd.concat([df_dup1, df_emails, null_df], ignore_index=False)

        df = df.loc[indexes]
        df.drop_duplicates(inplace=True)
        df.dropna(inplace=True)

        # for i, j in zip(df['Date'].values, df['Date'].index):
        #     df['Date'][j] = re.sub("[^0-9]", "/", str(i))
        #
        # df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')

        test_indexes = []
        indexes = []
        for i, j in zip(df['Account Name'].values, df['Account Name'].index):
            if type(i) == str:
                i = str.lower(i)
                x = re.findall(r'demo|test|sample', i)
                if (x):
                    test_indexes.append(j)
                else:
                    indexes.append(j)

        dup2 = []
        for i, j in zip(df.duplicated().values, df.duplicated().index.values):
            if i:
                dup2.append(j)
                indexes.remove(j)

        df_dup2 = df.loc[dup2]

        data_copy2 = pd.concat([data_copy, df_dup2])

        df.drop_duplicates(inplace=True)

        test_df = df.loc[test_indexes]

        main_df = df.loc[indexes]

        data = pd.concat([main_df, test_df, data_copy])

        json_compatible_item_data = jsonable_encoder(main_df)
        return JSONResponse(content=json_compatible_item_data)
        # return {"data": "Processing Data"}


@app.get("/")
async def main():
    content = """
<body>
<form action="/csvdata/" enctype="multipart/form-data" method="post">
<input name="files" type="file" multiple>
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
