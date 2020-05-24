import couchdb
import pandas as pd
import json


def read_json(file_name):
    with open(file_name, 'r') as json_file:
        aurindata = json.load(json_file)
    return aurindata


def aurin_data_reformat(aurin_data):
    # create a new dict
    format_data = {}
    for each in aurin_data['features']:
        if each['properties']['earners_persons'] is not None:
            format_data[each['properties']['sa3_code_2016']] = {
                '< $18378': float(each['properties']['lowest_quartile_pc']),
                '$18378-$40369': float(each['properties']['second_quartile_pc']),
                '$40369-$68106': float(each['properties']['third_quartile_pc']),
                '> $68106': float(each['properties']['highest_quartile_pc']),
                'median age of earner': int(each['properties']['median_age_of_earners_years'])
            }
    format_data = pd.DataFrame(format_data).T
    format_data = format_data.reset_index()
    return format_data


def code_to_state(format_data):
    translate = {}
    with open('SA3_2016_AUST.csv', 'r') as sa3_data:
        for area in sa3_data.readlines()[1:]:
            area = area.split(',')
            translate[area[0]] = area[7]
    format_data['index'] = format_data['index'].replace(translate)
    return format_data


def aurin_data_analysis():
    aurin_data = read_json('SA3 personal income.json')
    format_data = aurin_data_reformat(aurin_data)
    format_data = code_to_state(format_data)
    aurin_median = format_data.groupby(['index']).median()
    aurin_final = format_data.groupby(['index']).max()
    aurin_middle_class = aurin_final['$40369-$68106'] + aurin_final['> $68106']
    return aurin_median, aurin_middle_class


def connect_db(url):
    try:
        couch = couchdb.Server(url)
        return couch
    except:
        print('error')


def get_view(database_name, view_name, couch):
    try:
        db = couch[database_name]
        res = db.view(view_name, group='true')
        return res
    except:
        print('Error. No such view.')


def reformat_twitter_data():
    audata = read_json('au.json')
    state = {}
    for info in audata:
        admin = info['admin']
        if admin in state.keys():
            state[admin].append(info['city'])
        else:
            state[admin] = []
            state[admin].append(info['city'])
    return state


def find_loc(address):
    state = reformat_twitter_data()
    for state_name in state.keys():
        if address.find(state_name) != -1:
            return state_name

        for city_name in state[state_name]:
            if address.find(city_name) != -1:
                return state_name
    return None


def twitter_data_analysis(view):
    df = pd.DataFrame(view)
    df['retweet'] = df['value'].apply(lambda x: x['retweet'])
    df['count'] = df['value'].apply(lambda x: x['count'])
    del df['value']
    df['location'] = df['key'].apply(find_loc)
    del df['key']
    tweet_final = df.groupby(['location']).sum()
    tweet_final['freq of retweet'] = tweet_final['retweet'] / tweet_final['count']
    return tweet_final


def output_result(tweet_final, aurin_median, aurin_middle_class):
    out = {}
    for loc in tweet_final.index:
        out[loc] = {
            'retweet number': str('%d' % tweet_final['retweet'][loc]),
            'tweets number': str('%d' % tweet_final['count'][loc]),
            'freq of retweet': str('%.3f' % tweet_final['freq of retweet'][loc]),
            'percentage of middle&upper class': str('%.1f' % aurin_middle_class[loc]),
            'median age of earner': str('%d' % aurin_median['median age of earner'][loc])
        }
    return out


def save_tweet(name, newdata, couch):
    try:
        database = couch[name]
        newdata['_id'] = 'retweet'
        newdata['_rev'] = database['retweet'].rev
        database.save(newdata)
    except:
        print("Creating database", name)
        database = couch.create(name)
        newdata['_id'] = 'retweet'
        database.save(newdata)


def main():
    print('*********************Aurin Data Analysis*********************************')
    aurin_median, aurin_middle_class = aurin_data_analysis()
    print('*********************Connecting to DataBase*********************************')
    url = 'http://admin:1111@172.26.130.31:5984/'
    couch = connect_db(url)
    print('*********************Tweets Data Analysis*********************************')
    res = get_view('tweets', "sentiment/retweet", couch)
    tweet_final = twitter_data_analysis(res)
    output = output_result(tweet_final, aurin_median, aurin_middle_class)
    print('*********************Output*********************************')
    save_tweet('retweet_output', output, couch)

    
if __name__ == '__main__':
    main()
