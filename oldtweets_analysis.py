import couchdb
import pandas as pd
import json
from textblob import TextBlob
from collections import Counter


def connect_todb(url):
    try:
        couch = couchdb.Server(url)
        return couch
    except:
        print('error')


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


def format_data(df):
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
        print(each_city, len(df))
        final.append(df)
    for idx in range(len(final)):
        final[idx] = format_data(final[idx])
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
                stats[state].append(
                    {'month': mon, 'negative_rate': '%.4f' % float(group_percent[state, sentiment]),
                     'positive_rate': '%.4f' % float(group_percent[state, 'positive']),
                     'common_tag': dic[state],
                     'percentage of middle&upper class': str('%.1f' % aurin_middle_class[state])})
            else:
                stats[state] = []
                stats[state].append(
                    {'month': mon, 'negative_rate': '%.4f' % float(group_percent[state, sentiment]),
                     'positive_rate': '%.4f' % float(group_percent[state, 'positive']),
                     'common_tag': dic[state],
                     'percentage of middle&upper class': str('%.1f' % aurin_middle_class[state])})
    return stats


def controller(mon, data, stats, aurin_middle_class):
    stage_data = data[data['date'].apply(lambda x: x.month) == mon].copy()
    stage_data.reset_index(drop=True, inplace=True)
    stats = data_analysis(stage_data, mon, stats, aurin_middle_class)
    return stats


def save_tweet(name, newdata, couch, docname):
    try:
        database = couch[name]
        for _id in database:
            if _id == docname:
                newdata['_id'] = docname
                newdata['_rev'] = database[docname].rev
                database.save(newdata)
                return
        newdata['_id'] = docname
        database.save(newdata)

    except:
        print("Creating database", name)
        database = couch.create(name)
        newdata['_id'] = docname
        database.save(newdata)


def main():
    print('*********************Aurin Data Analysis*********************************')
    aurin_middle_class = aurin_data_analysis()

    print('*********************Connect to DataBase*********************************')
    url = 'http://admin:1111@172.26.130.31:5984/'
    couch = connect_todb(url)

    print('*********************Retrieving Old Tweets*********************************')
    li = ['australiancapitalterritory_tweets', 'northernterritory_tweets', 'newsouthwales_tweets', 'queensland_tweets',
          'victoria_tweets', 'southaustralia_tweets', 'tasmania_tweets', 'westernaustralia_tweets']
    month = [9, 10, 11, 12, 1, 2, 3, 4]

    for each in li:
        db = couch[each]
        data = db.view("Morrison/view3")
        df = pd.DataFrame(data)
        final = format_data(df)
        stats = {}
        for mon in month:
            stats = controller(mon, final, stats, aurin_middle_class)
        docname = each.strip('_tweets')
        save_tweet('oldtweets_output', stats, couch, docname)


if __name__ == '__main__':
    main()
