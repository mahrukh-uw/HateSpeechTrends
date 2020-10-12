import dash
import dash_html_components as html
import dash_core_components as dcc
import plotly.graph_objs as go
import pandas as pd
from read_from_cassandra import read_daily_table
from plotly.subplots import make_subplots
import plotly.graph_objs as go
import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
###############################
# Pulling data from Cassandra #
###############################

def get_data(table_name,col='date',asc=True):
    today = pd.datetime.today()
    #print (today)
    #2018-01-09 10:51:42.701585


    df=read_daily_table(table_name)
    df['date']=pd.to_datetime(df['date'], errors='coerce')
    df = df[df['date'].between('06-01-2016', '06-01-2017')]#,today)]
    df=df.sort_values(by=col,ascending=asc)
    print(table_name,df.shape)
    return df


###########
#Plotting #
###########


app = dash.Dash(__name__)
app.scripts.config.serve_locally = True
app.css.config.serve_locally = True
application = app.server

text_style = dict(color='#444', fontFamily='sans-serif', fontWeight=300, fontSize=36)
title = 'Daily Hate Trends.'


# Get trends data
#df = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/finance-charts-apple.csv")
df=get_data('dailytrend')
df2=get_data('overalldailytrend')
df2.columns=['date','overallcount']
df.columns=['date','hatecount']
df_merged=df2.merge(df, how='left',on='date')
df_merged=df_merged.fillna(0)
#print(df_merged.head())

trace_high = go.Scatter(
    x=df_merged.date,
    y=df_merged['overallcount'],
    name = "Overall Count",
    line = dict(color = '#17BECF'),
    opacity = 0.8)


trace_low = go.Scatter(
    x=df_merged.date,
    y=df_merged['hatecount'],
    name = "Toxic Count",
    line = dict(color = '#7F7F7F'),
    opacity = 0.8)

years = ['2016','2017','2018']


top_users=get_data('topuser',col='count',asc=False)

top_users2=top_users.groupby('username', as_index=False)['count'].sum()
top_users2=top_users2.sort_values(by='count',ascending=False)
top_users2=top_users2.head(50)
trace_bar=go.Bar(base=0, x=top_users2['username'],
                y=top_users2['count'],
                marker_color='#17BECF',
                name='Tweets')

data = [trace_high]#,trace_low]

layout1 = dict(
    title='Overall tweets per day ',
    xaxis=dict(
        rangeselector=dict(
            buttons=list([
                dict(count=1,
                     label='1m',
                     step='month',
                     stepmode='backward'),
                dict(count=6,
                     label='6m',
                     step='month',
                     stepmode='backward'),
                dict(step='all')
            ])
        ),
        rangeslider=dict(),
        type='date'
    )
)
layout2=layout1.copy()
layout2['title']='Toxic tweets per day'
fig = dict(data=data, layout=layout1)

fig2=dict(data=[trace_low],layout=layout2)



fig3 = dict(
        data= [trace_bar],
            layout= {"title": {"text": "Users with most hateful tweet"}}
            )

# To display the figure defined by this dict, use the low-level plotly.io.show function
import plotly.io as pio

# Now here's the Dash part:

app.layout = html.Div([
    dcc.Graph(id='overall-tweets', figure=fig),

    dcc.Graph(id='hate-tweets', figure=fig2),
    dcc.Graph(id='bar-graph', figure=fig3)
])



'''
app.layout = html.Div(
    [html.H2(title, style=text_style),
     html.Div([
         dcc.Dropdown(
             id='tag-dropdown',
             options=[{'label': tag, 'value': tag} for tag in all_tags],
             #value=all_tags[0],
             placeholder="Select a Stackoverflow Tag.",
             optionHeight=18
         ),
     ], style={'width': '20%', 'display': 'inline-block', 'fontColor': 'blue'}),
     html.Div([
         dcc.Dropdown(
             id='keyword-dropdown',
             placeholder="Now select an associated keyword.",
             optionHeight=18
         ),
     ], style={'width': '20%', 'display': 'inline-block'}
     ),
     html.Hr(),
     html.Div(id='display-selected-values'),
     ]
)


@app.callback(
    dash.dependencies.Output('keyword-dropdown', 'options'),
    [dash.dependencies.Input('tag-dropdown', 'value')])
def update_date_dropdown(tag):
    print('Got {} as tag'.format(tag))
    print(keys_dict)
    return [{'label': i, 'value': i} for i in keys_dict[tag]]


@app.callback(
    dash.dependencies.Output('display-selected-values', 'children'),
    [dash.dependencies.Input('keyword-dropdown', 'value'),
     dash.dependencies.Input('tag-dropdown', 'value')])
def display_graph(selected_value_1, selected_value_2):
    if selected_value_1 and selected_value_2:
        dates = read_one_from_cassandra(selected_value_2, selected_value_1)
        return dcc.Graph(figure=go.Figure(data=[go.Bar(x=datetime_x_y(dates)[0],
                                                       y=datetime_x_y(dates)[1],
                                                       marker=go.bar.Marker(color='rgb(26, 118, 255)')
                                                       )], layout={
            'title':'Count of uses of the keyword: {0} within posts tagged with: {1}'.format(selected_value_1, selected_value_2),
        'yaxis': {'title':'Count per month'}
        }
                                          ))
    else:
        pass
'''

if __name__ == '__main__':
    #application.run(host=os.environ["DASH_DNS"], port=80)
    application.run(host='0.0.0.0', port=8050, debug=True)
