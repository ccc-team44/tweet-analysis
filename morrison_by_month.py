import couchdb
import pandas as pd
import json
from textblob import TextBlob
from collections import Counter
import re


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
    aurin_final = format_data.groupby(['index']).max()
    aurin_middle_class = aurin_final['$40369-$68106'] + aurin_final['> $68106']
    return aurin_middle_class


def connect_todb(url):
    try:
        couch = couchdb.Server(url)
        return couch
    except:
        print('error')


def format_data(df):
    df.drop_duplicates('key', 'first', inplace=True)
    df['city'] = df['value'].apply(lambda x: x['city'])
    df['state'] = df['value'].apply(lambda x: x['state'])
    df['hashtags'] = df['value'].apply(lambda x: x['hashtags'])
    df['date'] = df['value'].apply(lambda x: x['date'].split()[0])
    df['date'] = df['date'].apply(pd.Timestamp)
    df['text'] = df['value'].apply(lambda x: x['text'])
    del df['key']
    del df['value']
    del df['id']
    return df


def read_format_view(li, couch):
    final = []
    for each_city in li:
        db = couch[each_city]
        res = db.view("Morrison/view3")
        df = pd.DataFrame(res)
        final.append(df)
    for each in final:
        each = format_data(each)
    for each in final[1:]:
        final[0] = final[0].append(each)
    final_data = final[0].copy()
    final_data = final_data.reset_index(drop=True)
    return final_data


def predict_sentiment(sentence):
    sentiment = TextBlob(sentence)
    if sentiment.sentiment_assessments.polarity > 0:
        return 'positive'
    elif sentiment.sentiment_assessments.polarity < 0:
        return 'negative'
    else:
        return 'neural'


def count_hashtag_by_city(df):
    dic = {}
    for each in df.groupby(['state'])['hashtags']:
        counter = Counter()
        for tag in each[1]:
            for token in tag:
                if token != '':
                    counter[token.lower()] += 1
        dic[each[0]] = counter.most_common(6)
    return dic


def data_analysis(df, mon, stats, aurin_middle_class):
    df['sentiment'] = df['text'].apply(lambda x: predict_sentiment(x))
    # df = df[df['sentiment']!= 'neural']
    group_data = df.groupby(['state', 'sentiment'])['text'].count()
    group_total = df.groupby(['state'])['text'].count()
    group_percent = group_data.div(group_total)
    dic = count_hashtag_by_city(df)

    for (state, sentiment) in group_percent.index:
        if sentiment == 'negative':
            if state in stats.keys():
                try:
                    pos = float(group_percent[state, 'positive'])
                    stats[state].append(
                        {'month': mon, 'negative_rate': '%.4f' % float(group_percent[state, sentiment]),
                         'positive_rate': '%.4f' % pos,
                         'common_tag': dic[state],
                         'percentage of middle&upper class': str('%.1f' % aurin_middle_class[state])})
                except:
                    pos = float(0)
                    stats[state].append(
                        {'month': mon, 'negative_rate': '%.4f' % float(group_percent[state, sentiment]),
                         'positive_rate': '%.4f' % pos,
                         'common_tag': dic[state],
                         'percentage of middle&upper class': str('%.1f' % aurin_middle_class[state])})
            else:
                stats[state] = []
                try:
                    pos = float(group_percent[state, 'positive'])
                    stats[state].append(
                        {'month': mon, 'negative_rate': '%.4f' % float(group_percent[state, sentiment]),
                         'positive_rate': '%.4f' % pos,
                         'common_tag': dic[state],
                         'percentage of middle&upper class': str('%.1f' % aurin_middle_class[state])})
                except:
                    pos = float(0)
                    stats[state].append(
                        {'month': mon, 'negative_rate': '%.4f' % float(group_percent[state, sentiment]),
                         'positive_rate': '%.4f' % pos,
                         'common_tag': dic[state],
                         'percentage of middle&upper class': str('%.1f' % aurin_middle_class[state])})
    return stats


def controller(mon, data, stats, aurin_middle_class):
    stage_data = data[data['date'].apply(lambda x: x.month) == mon].copy()
    stage_data.reset_index(drop=True, inplace=True)
    stats = data_analysis(stage_data, mon, stats, aurin_middle_class)
    return stats


def find_hashtags(sentence):
    pattern = re.compile('#\w+')
    match = re.findall(pattern, sentence)
    return match


def reformat_location_data():
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
    state = reformat_location_data()
    for state_name in state.keys():
        if address.find(state_name) != -1:
            return state_name

        for city_name in state[state_name]:
            if address.find(city_name) != -1:
                return state_name
    return None


def format_newdata(df):
    state = {'sydney': 'New South Wales', 'melbourne': 'Victoria', 'perth': 'Western Australia',
             'adelaide': 'South Australia', 'brisbane': 'Queensland'}
    df.drop_duplicates('key', 'first', inplace=True)
    df.reset_index(drop=True, inplace=True)
    df['city'] = df['value'].apply(lambda x: x['city'])
    df['state'] = df['city'].replace(state)
    df['date'] = pd.Timestamp('2020-5')
    df['text'] = df['value'].apply(lambda x: x['text'])
    df['hashtags'] = df['text'].apply(lambda x: find_hashtags(x))
    del df['value']
    del df['key']
    del df['id']
    return df


def format_stream_data(df):
    df.drop_duplicates('key', 'first', inplace=True)
    df.reset_index(drop=True, inplace=True)
    df['city'] = df['value'].apply(lambda x: x['city'])
    df['state'] = df['value'].apply(lambda x: find_loc(x['city']))
    df['date'] = pd.Timestamp('2020-5')
    df['text'] = df['value'].apply(lambda x: x['text'])
    df['hashtags'] = df['text'].apply(lambda x: find_hashtags(x))
    df.dropna(inplace=True)
    del df['value']
    del df['key']
    del df['id']
    return df


def save_out(name, newdata, couch):
    try:
        database = couch[name]
        for each in database:
            _rev = database[each].rev
            newdata['_rev'] = _rev
            newdata['_id'] = each
            database.save(newdata)
    except:
        print("Creating database", name)
        database = couch.create(name)
        database.save(newdata)


if __name__ == '__main__':
    print('*********************Aurin Data Analysis*********************************')
    aurin_middle_class = aurin_data_analysis()
    print('*********************Connect to DataBase*********************************')
    url = 'http://admin:1111@172.26.130.31:5984/'
    couch = connect_todb(url)
    print('*********************Retrieving Old Tweets*********************************')
    li = ['australiancapitalterritory_tweets', 'northernterritory_tweets', 'newsouthwales_tweets', 'queensland_tweets',
          'victoria_tweets', 'southaustralia_tweets', 'tasmania_tweets', 'westernaustralia_tweets']
    data = read_format_view(li, couch)
    month = [9, 10, 11, 12, 1, 2, 3, 4]
    output = []
    stats = {}
    print('*********************Analyzing Data*********************************')
    for mon in month:
        stats = controller(mon, data, stats, aurin_middle_class)
    print('*********************Retrieving Location Tweet*********************************')
    li = ['sydney', 'melbourne', 'perth', 'adelaide', 'brisbane']
    df = pd.DataFrame()
    for each_loc in li:
        db = couch[each_loc]
        auspol = db.view("Morrison/aupol")
        scomo = db.view("Morrison/scomo")
        df = df.append(pd.DataFrame(auspol).append(pd.DataFrame(scomo)))
    df = format_newdata(df)
    print('*********************Retrieving Streaming Tweets*********************************')
    db = couch['tweets']
    auspol = db.view("Morrison/aupol")
    scomo = db.view("Morrison/scomo")
    temp = pd.DataFrame(auspol).append(pd.DataFrame(scomo))
    temp.reset_index(drop=True, inplace=True)

    for idx in range(len(temp)):
        if 'text' not in temp.loc[idx]['value'].keys():
            temp.drop(idx, inplace=True)
    temp.reset_index(drop=True, inplace=True)
    temp = format_stream_data(temp)
    temp.reset_index(drop=True, inplace=True)
    df = df.append(temp)
    print('*********************Analyzing Data*********************************')
    stats = data_analysis(df, mon=5, stats=stats, aurin_middle_class=aurin_middle_class)
    print('*********************Saving Output*********************************')
    save_out('morrison_output_test', stats, couch)
