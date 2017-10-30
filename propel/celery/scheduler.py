from propel import configuration 
from propel.celery import app 

@app.task
def schedule():
    
    
scheduler_frequency = configuration.get('scheduler_frequency')
schedule_dict = {}
for item in scheduler_frequency:
    schedule_dict[item[0]] = item[1]
    
        
app.conf.beat_schedule = {
    'schedule': {
        'task': 
        }
    }     