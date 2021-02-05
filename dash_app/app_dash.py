import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import configparser

from rds_db import query_rds

app = dash.Dash(__name__)
TOP_N_TERRITORIES = 5
#query the max date once only
MAX_DATE = query_rds('SELECT MAX(dt) dt FROM dim.time')['dt'].iloc[0]


def __query_territories(territories):
    """
    Formats the query based on number of territories selected
    """
    if len(territories) > 1:
        return f'WHERE combined_key IN {tuple(territories)}'
    return f"WHERE combined_key = '{territories[0]}'"


def __query_highest(territory_level, metric, cond_date):
    """
    Formats the 'n highest' query.

    bi_top tables are half the size of the regular bi tables since locations with populations below the median (50th percentile) in their respective location levels are excluded in these tables.
    """

    # the 'WHERE TRUE' does not effect the query, but makes the 'cond_date' more easily template-able
    query = f"""
     JOIN
        (SELECT location_id, SUM({metric}) total
        FROM bi.bi_{territory_level}_top
        WHERE TRUE {cond_date}
        GROUP BY location_id
        ORDER BY total DESC LIMIT 5) tmp
    on bi.location_id = tmp.location_id
    """
    return query


def __query_location_ids(territories):

    return f"WHERE location_id IN (SELECT DISTINCT location_id FROM dim.location {__query_territories(territories)})"


def __query_date(time_period):
    """ Formats query based on selected time period """
    time_period = time_period.lower()
    if time_period == 'all time':
        return ""
    else:
        days = int(time_period[:2])
        return f" AND (dt > TO_DATE('{MAX_DATE}', 'YYYY-MM-DD') - INTERVAL '{days} day')"


def __sort_query(territory_level):
    """
    Returns correct sort order based on selected territory level
    i.e if user selects county, then the list of counties is
    sorted by country, state, and finally counties
    """
    sort_by = {"county": "country, state, county", "state": "country, state", "country": "combined_key"}
    return f"{sort_by[territory_level]}"


def __generate_query(territory_level, territories, metric, per_capita_calc, time_period):
    """
    Based on user selected options, generate the formatted query string
    """
    cond_date = __query_date(time_period)

    # user selected top n territories
    if territories is None:
        cond_territory = __query_highest(territory_level, metric, cond_date)
        order_by = "ORDER BY total DESC, dt"
        top = '_top'

    #user selected specific territories
    else:
        cond_territory = __query_location_ids(territories)
        order_by = "ORDER BY dt"
        top = ''

    base_query = f"SELECT bi.combined_key, dt, {metric} FROM bi.bi_{territory_level}{top} bi"
    tail_query = f" {cond_territory} {cond_date} {order_by}"
    query = base_query + tail_query
    print(f'\n{query}')
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

    # if user selected top n territories
    if territories is None:
        territories = df_gb['combined_key'].unique()
    for territory in territories:
        df_territory = df_gb.loc[df_gb['combined_key'] == territory]
        trace = go.Scatter(x=df_territory['dt'], y=df_territory[metric], name=territory)
        traces.append(trace)
    return traces


def __get_territory_options(territory_level):
    """
    Based on user selected territory level,
    returns a list of sorted territories.

    The territories are in longform format,
    i.e for counties, county_state_country is returned
    """

    sort_by = __sort_query(territory_level)
    #have to alias combined_key else countries is an invalid query- select combined_key, combined_key
    query = f"SELECT c_key as combined_key FROM (SELECT DISTINCT combined_key c_key, {sort_by} FROM bi.bi_{territory_level} ORDER BY {sort_by}) tmp"
    df = query_rds(query)
    return list(df.iloc[:, 0])


def get_territory_options_formatted():
    """
    Formats list of territory options per Dash dropdown standards.
    Example of returned data structure:
    {'country': [{'label': 'Algeria', 'value': 'Algeria'}, {'label': 'Angola', 'value': 'Angola'}], 'state': [{'label': 'Alberta, Canada', 'value': 'Alberta, Canada'}, {'label': 'Anhui, China', 'value': 'Anhui, China'}]}
    """
    territory_options = {
        territory_level:
        [
            {'label': territories, 'value': territories}
            for territories in __get_territory_options(territory_level)
        ]
        for territory_level in territory_level_options
    }
    return territory_options


# --------------- User options ------------

territory_level_options = ('country', 'state', 'county')
#get territory options once rather than every single time user selects a new territory level
territory_options = get_territory_options_formatted()
metric_options = ('confirmed', 'deaths', 'recovered')
per_capita_options = ('actual', 'per capita')
time_options = ('all time', '30 days', '90 days')


# --------------- App layout ------------
colors = {'bg_text': '#332F2E', 'bg': '#ebf5f6', 'dropdown_border': '#767676'}
app.layout = html.Div(children=[
    # Main title of the page
    html.H1(
        children='COVID-19 Case Comparison By Location',
        style={
            'textAlign': 'center',
            'color': colors['bg_text']
        }
    ),

    #The top level div for all elements in the blue banner
    html.Div(children=[
        #Second level div for the first column in the blue banner, most notably the dropdown and html button
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
                    f'Show Top {TOP_N_TERRITORIES}', 'top_n_button',
                    style={
                        'height': '36px',
                        'border-radius': '20px',
                        'padding-left': '30px',
                        'padding-right': '30px'
                    }
                )
            ],
            #style for the 1st column, last row only
            style={'display': 'flex', 'margin-left': '10px', 'margin-right': '70px'}
            )
        ],
        #style for all of the first column
        style={'display': 'flex', 'flex-direction': 'column', 'padding-bottom': 10}
        ),

        # Div for 2nd column in blue banner
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

        # Div for 3rd column in blue banner
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

        # Div for 4th column in blue banner
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

        #style for time period column
        style={'margin-right': 50})
    ],
    #style for all elements in blue banner
    style={'display': 'flex', 'backgroundColor': colors['bg'], 'padding-left': 20}),


    dcc.Graph(
        id='graph'
    ),

    # Additional info text at the bottom
    html.Div(
        children=[
            html.P(
                children=[
                    '* Source data: ',
                    html.A(
                        children='https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series',
                        href='https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series')
                ]
            ),

            html.P(
                children='** Per capita is per 100k people'
            ),

            html.P(
                children=f'*** Top {TOP_N_TERRITORIES} metric is calculated as locations with highest sum totals for selected metric and time period. Additionally, locations with populations below the median (50th percentile) in their respective location levels are excluded.'
            ),

            html.P(
                children='**** 7 day moving average has been applied to all metrics'
            ),

            html.P(
                children='***** Recovered metric is missing for the US for all levels'
            ),
        ],
        #style for all informational text
        style={'textAlign': 'left', 'margin-left': 60, 'font-size': '.8em'}
    )
],
)

# ------------- Define App Interactivity ----------

# global variables to keep track of changes
global prev_territory_level
prev_territory_level = ''

global prev_n_clicks
prev_n_clicks = 0


@app.callback(
    Output('territory_drop', 'value'),
    [
        Input('territory_level_radio', 'value'),
        Input('top_n_button', 'n_clicks')
    ]
)
def set_territory_value(territory_level, n_clicks):
    #print(f'\ndash.callback_context.triggered: {dash.callback_context.triggered}')
    triggered = dash.callback_context.triggered[0]['prop_id']
    if triggered == 'territory_level_radio.value' or triggered == 'top_n_button.n_clicks':
        return None
    raise dash.exceptions.PreventUpdate


@app.callback(
    [
        Output('territory_drop', 'options'),
        Output('territory_drop', 'placeholder'),
    ],
    [
        Input('territory_level_radio', 'value')
    ]
)
def set_territory_options(territory_level):
    """ Set territory options in the dropdown

    Kwargs:
    territory_level -- The user selected territory level, i.e county, state, or country

    Return values:
    territory dropdown options -- the list of all territories to choose from based on selected territory level.

    territory_placeholder -- the placeholder msg to display if user has not selected a territory yet.

    territory_value -- Set or possibly reset the user selected territory

    """
    global prev_territory_level
    refresh_territory_level = dash.callback_context.triggered[0]['value']
    #print(f'\n set_territory_options(), dash.callback_context: {dash.callback_context.triggered}')
    #update dropdown values on refresh or on updated user selection of territory level
    if refresh_territory_level is None or prev_territory_level != territory_level:
        #print(f'adjusting dropdown for territory level: {territory_level}')
        prev_territory_level = territory_level
        # return a new list of territory options, and a new placeholder msg
        # Also, reset territory_value to None. This one is tricky because in the dropdown UI, the value is cleared, but the actual Input still exists meaning the graph callback is needlessly called
        return territory_options[territory_level], f'Select {territory_level}(s)...'


#creates graph based on all user inputs
@app.callback(
    Output('graph', 'figure'),
    [
        Input('territory_level_radio', 'value'),
        Input('territory_drop', 'value'),
        Input('metrics_radio', 'value'),
        Input('per_capita_radio', 'value'),
        Input('time_period_radio', 'value'),
        Input('top_n_button', 'n_clicks')
    ])
def update_graph(territory_level, territories, metric, per_capita_calc, time_period, n_clicks):
    """ Update graph based on all user inputs

    Kwargs:
    territory_level -- The user selected territory level, i.e county, state, or country.

    territories -- The user selected territories, i.e Angola, Algeria.

    metric -- The user selected metric, i.e confirmed.

    per_capita_calc -- either 'per capita' or 'actual' metric calculation.

    time_period -- The user selected time period, i.e 30 days.

    n_clicks -- Used to check to see if 'Show top 5' button was clicked

    Return values:
    Graph -- The rendered graph based on all user inputs
    """

    global prev_n_clicks
    #reset prev_n_clicks on refresh
    if n_clicks is None:
        prev_n_clicks = 0
    is_clicked = False
    if n_clicks and n_clicks != prev_n_clicks:
        is_clicked = True
        territories = None
        prev_n_clicks = n_clicks

    #print(f'\n update_graph(), dash.callback_context: {dash.callback_context.triggered}')
    # if the user has clicked 'Show top 5' or has selected a territory from the dropdown, then render graph
    if is_clicked or territories:
        #print(f'to graph: {territories}')
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

# run server
else:
    # Prefix is necessary for Dash to render correclty on Lambda API Gateway, but not necessary for custom domain using Route 53
    # more details here: https://github.com/Miserlou/Zappa/issues/2200#issuecomment-772702958
    app.config.update({
       'requests_pathname_prefix': '/dev/'
    })

    config = configparser.ConfigParser()
    config.read('config/dash_app.cfg')
    app.server.secret_key = config.get('ZAPPA', 'SECRET')
    server = app.server
