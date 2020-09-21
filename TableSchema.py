from datetime import datetime
class TableSchema:
    I94_SUB_SCHEMA = ['cicid', 
                      'i94yr', 
                      'i94mon', 
                      'i94res', 
                      'i94port', 
                      'arrdate',
                      'i94mode',
                      'i94addr', 
                      'depdate', 
                      'i94bir', 
                      'i94visa', 
                      'count', 
                      'occup',
                      'biryear', 
                      'gender', 
                      'airline', 
                      'admnum', 
                      'fltno', 
                      'visatype'
                      ]
    
    I94_MERGE_SCHEMA = ['cicid',
                        'admnum',
                        'i94port',
                        'i94yr',
                        'i94mon',
                        'count', 
                        'gender', 
                       ]
    
    AIRPORT_SUB_SCHEMA = ['Ident',
                          'City',
                          'State'
                         ]
    
    WORLD_TEMP_SCHEMA = ['dt',
                         'AverageTemperature',
                         'City',
                         'Country',
                         'Latitude',
                         'Longitude'
                         ]
    
    WORLD_TEMP_DATE = datetime(2013,4,1)
    
    US_DEMO_SCHEMA = ['City',
                      'State',
                      'Median Age',
                      'Male Population',
                      'Female Population',
                      'Total Population',
                      'Number of Veterans',
                      'Foreign-born',
                      'Average Household Size',
                      'State Code',
                      'Race',
                      'Count'
                      ]
    
    US_DEMO_MERGE_SCHEMA = ['City',
                            'State',
                            'Median Age',
                            'Male Population',
                            'Female Population',
                            'Total Population',
                            'Number of Veterans',
                            'Foreign-born',
                            'Average Household Size',
                            'State Code'
                            ]   
    
    FACT_DROP_COLUMN = ['State Code',
                        'State_x',
                        'State_y',
                        'Latitude',
                        'Longitude'
                        ]