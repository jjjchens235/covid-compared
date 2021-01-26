import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import configparser

from rds_db import query_rds


def __query_territories(territories):
    if len(territories) > 1:
        return f'combined_key IN {tuple(territories)}'
    return f"combined_key = '{territories[0]}'"


def __query_highest(territory_level, metric):
    MIN_POPULATION = 50000
    return f'combined_key IN (SELECT combined_key FROM bi.bi_{territory_level} WHERE population > {MIN_POPULATION} AND {metric} IS NOT NULL GROUP BY combined_key ORDER BY sum({metric}) DESC LIMIT {TOP_N})'


def __query_date(time_period):
    time_period = time_period.lower()
    if time_period == 'all time':
        return ''
    else:
        max_date = query_rds('SELECT max(dt) dt FROM dim.time')['dt'].iloc[0]
        days = int(time_period[:2])
        return f"AND (dt > TO_DATE('{max_date}', 'YYYY-MM-DD') - INTERVAL '{days} day')"


def __sort_query(territory_level):
    """
    Sorts the selected list of territories
    i.e if user selects county, then the list of counties is
    sorted by country, state, and finally counties
    """
    sort_by = {"county": "country, state, county", "state": "country, state", "country": "combined_key"}
    return f"{sort_by[territory_level]}"


def __generate_query(territory_level, territories, metric, per_capita_calc, time_period):
    """
    Based on user selected options, generate the formatted query string
    """
    if territories is None:
        cond_territory = __query_highest(territory_level, metric)
    else:
        cond_territory = __query_territories(territories)
    cond_date = __query_date(time_period)

    query = f"SELECT combined_key, dt, {metric} FROM bi.bi_{territory_level} bi WHERE {cond_territory} {cond_date} ORDER BY dt"
    #print(query)
    return query


def get_traces(territory_level, territories, metric, per_capita_calc, time_period):
    """
    Query database for all territories user selected, then create seperate dataframes (traces) for each territory to be plotted
    """
    traces = []
    if per_capita_calc == 'per capita':
        metric = f'{metric}_per_capita'
    query = __generate_query(territory_level, territories, metric, per_capita_calc, time_period)
    df_gb = query_rds(query)
    #print(df_gb.tail())
    if territories is None:
        territories = df_gb['combined_key'].unique()
    for territory in territories:
        df_territory = df_gb.loc[df_gb['combined_key'] == territory]
        trace = go.Scatter(x=df_territory['dt'], y=df_territory[metric], name=territory)
        traces.append(trace)
    return traces


def get_territory_options(territory_level):
    """
    Based on user selected territory level,
    returns a list of sorted territories.

    The territories are in longform format,
    i.e for counties, county_state_country is returned
    """

    sort_by = __sort_query(territory_level)
    #have to alias combined_key else countries won't render correctly- select combined_key, combined_key
    query = f"SELECT c_key as combined_key FROM (SELECT DISTINCT combined_key c_key, {sort_by} FROM bi.bi_{territory_level} ORDER BY {sort_by}) tmp"
    df = query_rds(query)
    return list(df.iloc[:, 0])


app = dash.Dash(__name__) # , requests_pathname_prefix='/dev/')# , routes_pathname_prefix='/')

TOP_N = 5
colors = {'bg_text': '#332F2E', 'dropdown_border': '#767676', 'bg': '#ebf5f6'}

territory_level_options = ('country', 'state', 'county')
territory_options = {territory_level: get_territory_options(territory_level) for territory_level in territory_level_options}
metric_options = ('confirmed', 'deaths', 'recovered')
per_capita_options = ('actual', 'per capita')
time_options = ('all time', '30 days', '90 days')


app.layout = html.Div(children=[
    html.H1(
        children='covid-19 Case Comparison By Location',
        style={
            'textAlign': 'center',
            'color': colors['bg_text']
        }
    ),

    html.Div(children=[
        html.Div(children=[
            html.Div([
                html.P(
                    children='Location Level',
                    style={'textAlign': 'center', 'font-weight': 'bold', 'text-decoration': 'underline'}
                ),

                dcc.RadioItems(
                    id="territory_level_radio",
                    options=[{
                        'label': i,
                        'value': i
                    } for i in territory_level_options],
                    value='country',
                    style={'margin-bottom': 20, 'textAlign': 'center'}
                ),

                html.Div([
                    dcc.Dropdown(
                        id="territory_drop",
                        multi=True,
                        style={'border-color': colors['dropdown_border'], 'border-width': 2}
                    ),
                    html.P(
                        children='or',
                        style={'padding-left': 9, 'padding-right': 9, 'position': 'relative', 'bottom': 5}
                    ),
                    html.Button(
                        f'Show Top {TOP_N}', 'top_n_button',
                        style={
                            'height': '36px',
                            'border-radius': 20,
                            'padding-left': '30px',
                            'padding-right': '30px'
                        }
                    )
                ],
                style={'display': 'flex', 'margin-left': '10px', 'margin-right': '70px'}
                )
            ],
            style={'display': 'flex', 'flex-direction': 'column', 'padding-bottom': 10} # , 'width': '50%'}
            ),

            html.Div([
                html.P(
                    children='Metric',
                    style={'textAlign': 'center', 'font-weight': 'bold', 'text-decoration': 'underline'}
                ),

                dcc.RadioItems(
                    id="metrics_radio",
                    options=[{
                        'label': i,
                        'value': i
                    } for i in metric_options],
                    value='confirmed',
                    labelStyle={'display': 'flex', 'margin-bottom': 8}
                )
            ],
            style={'margin-right': 50}
            ),

            html.Div([
                html.P(
                    children='Calculation Method',
                    style={'textAlign': 'center', 'font-weight': 'bold', 'text-decoration': 'underline'}
                ),

                dcc.RadioItems(
                    id="per_capita_radio",
                    options=[{
                        'label': i,
                        'value': i
                    } for i in per_capita_options],
                    value='per capita',
                    labelStyle={'display': 'flex', 'margin-bottom': 8}
                )
            ],
            style={'margin-right': 50}),

            html.Div([
                html.P(
                    children='Time Period',
                    style={'textAlign': 'center', 'font-weight': 'bold', 'text-decoration': 'underline'}
                ),

                dcc.RadioItems(
                    id="time_period_radio",
                    options=[{
                        'label': i,
                        'value': i
                    } for i in time_options],
                    value='all time',
                    labelStyle={'display': 'flex', 'margin-bottom': 8}
                )
            ],

            style={'margin-right': 50})
        ],
        style={'display': 'flex'}
        )
    ],
    style={'backgroundColor': colors['bg'], 'padding-left': 20}),


    dcc.Graph(
        id='graph'
    ),
    html.Div(
        children=[
            html.P(
                children=[
                    '* Source data: ',
                    html.A(children='https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series', href='https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series'),
                ]
            ),
            html.P(
                children='** Per capita is per 100k people'
            ),
            html.P(
                children=f'*** Top {TOP_N} metric is calculated as locations with highest sum totals for selected metric during the selected time period'
            ),
            html.P(
                children='**** 7 day moving average has been applied to all metrics'
            ),
            html.P(
                children='***** Recovered metric is missing for the US for all levels'
            ),
        ],
        style={'textAlign': 'left', 'margin-left': 60, 'font-size': 14}
    )

],
)

global prev_territory_level
prev_territory_level = ''
global prev_n_clicks
prev_n_clicks = 0
# ------------- Define App Interactivity ----------


@app.callback([
    Output('territory_drop', 'options'),
    Output('territory_drop', 'placeholder'),
    Output('territory_drop', 'value')],
    Input('territory_level_radio', 'value'))
def set_territory_options(territory_level):
    """
    territory_level value is input, the territory options is the output
    """
    global prev_territory_level
    refresh_territory_level = dash.callback_context.triggered[0]['value']
    #before updating options, wait until the page has been refreshed or the territory level has changed
    if refresh_territory_level is None or prev_territory_level != territory_level:
        prev_territory_level = territory_level
        return [{'label': i, 'value': i} for i in territory_options[territory_level]], f'Select {territory_level}(s)...', None


#creates graph based on all user inputs
@app.callback(
    dash.dependencies.Output('graph', 'figure'),
    [dash.dependencies.Input('territory_level_radio', 'value'), dash.dependencies.Input('territory_drop', 'value'), dash.dependencies.Input('metrics_radio', 'value'), dash.dependencies.Input('per_capita_radio', 'value'), dash.dependencies.Input('time_period_radio', 'value'), Input('top_n_button', 'n_clicks')])
def update_graph(territory_level, territories, metric, per_capita_calc, time_period, n_clicks):

    global prev_n_clicks
    is_clicked = False
    if n_clicks and n_clicks != prev_n_clicks:
        is_clicked = True
        territories = None
        prev_n_clicks = n_clicks

    if is_clicked or territories:
        traces = get_traces(territory_level, territories, metric, per_capita_calc, time_period)

        return {
            'data': traces,
            'layout':
            go.Layout(title=f'{metric.upper()} ({per_capita_calc}) by day for {time_period.upper()}')
        }
    else:
        raise dash.exceptions.PreventUpdate


#run locally
if __name__ == '__main__':
    app.run_server(debug=True)

# run remotely
else:
    app.config.update({
       'url_base_pathname': '/dev',
       'routes_pathname_prefix': '',
       'requests_pathname_prefix': '/dev/'
   })
    app.css.config.serve_locally = False
    app.scripts.config.serve_locally = False

    config = configparser.ConfigParser()
    config.read('config/dash_app.cfg')
    app.server.secret_key = config.get('ZAPPA', 'SECRET')
    server = app.server
