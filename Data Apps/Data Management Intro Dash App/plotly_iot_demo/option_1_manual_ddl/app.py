import os
import json
from databricks import sql
from sqlalchemy.orm import declarative_base, Session, sessionmaker
from sqlalchemy import Column, String, Integer, BOOLEAN, create_engine, select, DATE, DATETIME, TIMESTAMP, DECIMAL, case, func, Table, MetaData
import ddls
from dash import Dash, html, Input, Output, ctx, dcc, dash_table
import pandas as pd
import plotly.express as px
import requests
import dash_bootstrap_components as dbc


with open('config.json') as w:

    conf = json.load(w)
    token = conf.get("token")
    http_path = conf.get("http_path")
    database = conf.get("database")
    host_name = conf.get("host_name")
    catalog = conf.get("catalog")



### Initialize Database Connection
conn_str = f"databricks://token:{token}@{host_name}?http_path={http_path}&catalog={catalog}&schema={database}"
extra_connect_args = {
            "_tls_verify_hostname": True,
            "_user_agent_entry": "PySQL Example Script",
        }
engine = create_engine(
            conn_str,
            connect_args=extra_connect_args,
        )



## Get Metadata from Config Files
#ddls.Base.metadata.create_all(bind=engine)
#ddls.Base.metadata.drop_all(bind=engine)

tables_stmt = f"""SELECT * FROM {catalog}.INFORMATION_SCHEMA.TABLES
WHERE table_schema = '{database}'"""


tables_in_db = pd.read_sql_query(tables_stmt, engine)

### Core Dash App

app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

### Layout

app.layout = html.Div([
    html.Button('Build Database Tables', id='build_db_btn', n_clicks=0),
    html.Button('Drop Database Tables', id='drop_tabes_btn', n_clicks=0),
    html.Button('Get Table List', id='fetch_tables_btn', n_clicks=0),
    html.Button('Run ELT Pipeline', id='run_etl_pipe', n_clicks=0),
    html.Div(id='container-button-timestamp'),
            html.Br(),
        dcc.RadioItems(
        id="checklist",
        options=["num_steps", "calories_burnt"],
        value="num_steps",
        inline=True,
        
    ),
        dcc.Graph(id='BasicSensors'),
        html.Div([html.Br(),
                 dcc.Graph(id='SmoothSensors')]),
        dash_table.DataTable(tables_in_db.to_dict('records'),[{"name": i, "id": i} for i in tables_in_db.columns], id='tbl'),
        html.Div(id='sch_tbl', className = 'sch_tbl')
    ])



@app.callback(Output('sch_tbl', 'children'),
              Input('tbl', 'data'),
              Input('tbl', 'active_cell'))
def update_graphs(data, active_cell):
    row_num = active_cell.get("row")
    col_num = active_cell.get("column")
    col_name = active_cell.get("column_id")

    table_name_to_detail = tables_in_db.loc[tables_in_db.index[row_num], col_name]

    if col_name == 'table_name':
        schema_stmt = f"""DESCRIBE TABLE EXTENDED {catalog}.{database}.{table_name_to_detail}"""
        schema_table = pd.read_sql_query(schema_stmt, engine)
        cols_for_data_table = [{'name': i, 'id': i} for i in schema_table.columns]

        res_table = dash_table.DataTable(
            id='table',
            columns=cols_for_data_table,
            data =schema_table.to_dict("rows"),
            style_data={
                'color': 'blue',
                'backgroundColor': 'white'
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': 'rgb(220, 220, 220)',
                }
            ],
            style_header={
                'backgroundColor': 'rgb(210, 210, 210)',
                'color': 'black',
                'fontWeight': 'bold'
            }
        )
        

        return res_table

    else:
        msg = f"Please select a table name... Currently Selected: {table_name_to_detail}"

        res_msg = dbc.Alert(msg, color="primary")

    return msg


## Sensors Chat Callback -- No Reflection
@app.callback(
    Output(component_id="BasicSensors", component_property="figure"),
    Input("checklist", "value")
)
def update_graph(yaxis):

    device_base_table = ddls.Base.metadata.tables["silver_sensors"]
    user_base_table = ddls.Base.metadata.tables["silver_users"]

    ## ORM-based SQL Query with dynamic filters in the callback

    userstmt = (select(device_base_table.c.timestamp, device_base_table.c.num_steps, device_base_table.c.calories_burnt)
                .limit(100)
        )
    

    ## Read data via pandas or just raw Dict/array
    ## TIPS: Always try to push the filtering/complex logic down to the system where the most data is filtered
    ## minimize data brought to client
    df = pd.read_sql_query(userstmt, engine).sort_values(by=['timestamp'])

    axis_labels = {
        "num_steps": "Total Daily Steps",
    }
    fig = px.line(
        df,
        x="timestamp",
        y=[f"{yaxis}"],
        markers=True,
        title=f"Comparative Daily Fitness Metrics by Demographic",
    )


    ## Build Plot Figure and return

    return fig



## Smooth Sensors Callback for Line Graph - Via Reflection
@app.callback(
    Output(component_id="SmoothSensors", component_property="figure"),
    Input("checklist", "value")
)
def update_smooth_graph(yaxis):

    if yaxis == "num_steps":
        chart_cols = ["SmoothedNumSteps30SecondMA", "SmoothedNumSteps120SecondMA"]

    elif yaxis == "calories_burnt":
        chart_cols = ["SmoothedCaloriesBurnt30SecondMA", "SmoothedCaloriesBurnt120SecondMA"]
        
    # Reflect database properties into ``metadata``.
    #ddls.Base.metadata.reflect(engine=engine)
    
    ## !! this table is NOT manually defined in our Python object, and is instead read on the fly with reflection
    sensors_table= Table("gold_sensors", 
                         ddls.Base.metadata, 
                         Column("timestamp", TIMESTAMP), 
                         autoload=True, 
                         autoload_with=engine,
                         extend_existing=True)
    
    # Instantiate a new ``FetchTable`` object to retrieve column objects by name.
    
    # Get a ``Column`` object from the desired ``Table`` object.
    yaxis_short_ma = sensors_table.columns[chart_cols[0]]
    yaxis_long_ma = sensors_table.columns[chart_cols[1]]
    time_col = sensors_table.columns["timestamp"]

    # Build a session-based query including filtering on text in ``column``.
    ma_statement = (select(time_col, yaxis_short_ma, yaxis_long_ma)
                .limit(100)
        )
    
    # Build a Pandas ``DataFrame`` with results from the query.
    df = pd.read_sql_query(ma_statement, engine).sort_values(by=['timestamp'], ascending=False)
    
    axis_labels = {
        "num_steps": "Total Daily Steps",
    }
    fig = px.line(
        df,
        x="timestamp",
        y=chart_cols,
        markers=True,
        title=f"Smoothed Moving Averages of Chosen Metric",
    )


    ## Build Plot Figure and return

    return fig


#### Run ELT Pipeline Callback
@app.callback(
    Output('container-button-timestamp', 'children'),
    Input('build_db_btn', 'n_clicks'),
    Input('drop_tabes_btn', 'n_clicks'),
    Input('fetch_tables_btn', 'n_clicks'),
    Input('run_etl_pipe', 'n_clicks')
)
def displayClick(btn1, btn2, btn3, btn4):
    msg = "No Database State Yet..."
    if 'build_db_btn' == ctx.triggered_id:

        ddls.Base.metadata.create_all(bind=engine)
        msg = "Database built!"

    elif 'drop_tabes_btn' == ctx.triggered_id:

        ddls.Base.metadata.drop_all(bind=engine)
        msg = "Database Dropped!"

    elif 'fetch_tables_btn' == ctx.triggered_id:
        tbls = list(ddls.Base.metadata.tables)
        msg = f"Here are the tables for {catalog}.{database}: {tbls}"

    elif 'run_etl_pipe' == ctx.triggered_id:

        ## Build and Trigger Databricks Jobs 
        job_req = {
                "name": "Plotly_Backend_Pipeline",
                "email_notifications": {
                    "no_alert_for_skipped_runs": "false"
                },
                "webhook_notifications": {},
                "timeout_seconds": 0,
                "max_concurrent_runs": 1,
                "tasks": [
                    {
                        "task_key": "Plotly_Backend_Pipeline",
                        "sql_task": {
                            "query": {
                                "query_id": "88c1412d-d2ca-43a1-9843-aec96b5b1586"
                            },
                            "warehouse_id": "ead10bf07050390f"
                        },
                        "timeout_seconds": 0,
                        "email_notifications": {}
                    }
                ],
                "format": "MULTI_TASK"
            }
        
        job_json = json.dumps(job_req)
        ## Get this from a secret or param
        headers_auth = {"Authorization":f"Bearer {token}"}
        uri = f"https://{host_name}/api/2.1/jobs/create"

        endp_resp = requests.post(uri, data=job_json, headers=headers_auth).json()

        ## Run Job
        job_id = endp_resp['job_id']

        run_now_uri = f"https://{host_name}/api/2.1/jobs/run-now"

        job_run = {"job_id": job_id }
        job_run_json = json.dumps(job_run)

        run_resp = requests.post(run_now_uri, data=job_run_json, headers=headers_auth).json()


        msg = f"Pipeline Created and Ran with Job Id: {endp_resp['job_id']} \n run message: {run_resp}"



    return html.Div(msg)




if __name__ == '__main__':
    app.run_server(debug=True)









