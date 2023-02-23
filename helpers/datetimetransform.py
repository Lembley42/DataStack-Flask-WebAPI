import datetime

def Transform_Dates_To_Datetime(data):
    if isinstance(data, dict):
        return {k: Transform_Dates_To_Datetime(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [Transform_Dates_To_Datetime(item) for item in data]
    elif isinstance(data, str):
        try:
            return datetime.datetime.strptime(data, '%Y-%m-%d')
        except ValueError:
            return data
    else:
        return data
